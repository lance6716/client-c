#include <kvproto/metapb.pb.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/pd/MockPDClient.h>

#include <initializer_list>
#include <map>
#include <memory>
#include <vector>

#include "test_helper.h"

namespace pingcap::tests
{
using namespace pingcap::kv;

namespace
{
struct PeerSpec
{
    uint64_t peer_id;
    uint64_t store_id;
};

Region makeRegion()
{
    metapb::Region meta;
    meta.set_id(1);
    meta.set_start_key("a");
    meta.set_end_key("b");
    auto * epoch = meta.mutable_region_epoch();
    epoch->set_conf_ver(1);
    epoch->set_version(1);

    metapb::Peer leader;
    leader.set_id(1);
    leader.set_store_id(1);
    return Region(meta, leader);
}

metapb::Peer makePeer(uint64_t peer_id, uint64_t store_id)
{
    metapb::Peer peer;
    peer.set_id(peer_id);
    peer.set_store_id(store_id);
    return peer;
}

metapb::Region makeMetaRegion(
    uint64_t id,
    const std::string & start_key,
    const std::string & end_key,
    uint64_t conf_ver,
    uint64_t version,
    std::initializer_list<PeerSpec> peers)
{
    metapb::Region meta;
    meta.set_id(id);
    meta.set_start_key(start_key);
    meta.set_end_key(end_key);
    auto * epoch = meta.mutable_region_epoch();
    epoch->set_conf_ver(conf_ver);
    epoch->set_version(version);
    for (const auto & peer : peers)
    {
        meta.add_peers()->CopyFrom(makePeer(peer.peer_id, peer.store_id));
    }
    return meta;
}

metapb::Store makeStore(uint64_t store_id)
{
    metapb::Store store;
    store.set_id(store_id);
    store.set_address("127.0.0.1:" + std::to_string(6000 + store_id));
    store.set_peer_address("127.0.0.1:" + std::to_string(7000 + store_id));
    store.set_state(metapb::StoreState::Up);
    return store;
}

class RegionCachePDClient : public pd::MockPDClient
{
public:
    RegionCachePDClient(const metapb::Region & region, const metapb::Peer & leader, std::initializer_list<uint64_t> store_ids)
        : region_(region)
        , leader_(leader)
    {
        for (uint64_t store_id : store_ids)
        {
            stores_.emplace(store_id, makeStore(store_id));
        }
    }

    pdpb::GetRegionResponse getRegionByKey(const std::string &) override { return makeRegionResponse(); }

    pdpb::GetRegionResponse getRegionByID(uint64_t region_id) override
    {
        if (region_id != region_.id())
        {
            throw Exception("unexpected region id", RegionUnavailable);
        }
        return makeRegionResponse();
    }

    metapb::Store getStore(uint64_t store_id) override
    {
        auto it = stores_.find(store_id);
        if (it == stores_.end())
        {
            throw Exception("unexpected store id", StoreNotReady);
        }
        return it->second;
    }

    std::vector<metapb::Store> getAllStores(bool) override
    {
        std::vector<metapb::Store> stores;
        for (const auto & [_, store] : stores_)
        {
            stores.push_back(store);
        }
        return stores;
    }

private:
    pdpb::GetRegionResponse makeRegionResponse() const
    {
        pdpb::GetRegionResponse resp;
        resp.mutable_region()->CopyFrom(region_);
        resp.mutable_leader()->CopyFrom(leader_);
        return resp;
    }

    metapb::Region region_;
    metapb::Peer leader_;
    std::map<uint64_t, metapb::Store> stores_;
};

std::unique_ptr<RegionCache> makeRegionCache(const pd::ClientPtr & pd_client)
{
    ClusterConfig config;
    config.tiflash_engine_key = "engine";
    config.tiflash_engine_value = "tiflash";
    return std::make_unique<RegionCache>(pd_client, config);
}
} // namespace

class RegionCacheTest : public testing::Test
{
};

TEST_F(RegionCacheTest, CheckRegionCacheTTL)
{
    Region::setRegionCacheTTL(/*base_sec=*/2, /*jitter_sec=*/0);
    Region::setRegionCacheTTLEnabled(true);

    int64_t ts = 1000;

    // expired
    {
        Region region = makeRegion();
        region.ttl.store(ts - 1);
        EXPECT_FALSE(region.checkRegionCacheTTL(ts));
    }

    // refresh on access
    {
        Region region = makeRegion();
        region.ttl.store(ts);
        EXPECT_TRUE(region.checkRegionCacheTTL(ts));
        EXPECT_EQ(region.ttl.load(), ts + 2);
    }

    // skip refresh when far away
    {
        Region region = makeRegion();
        region.ttl.store(ts + 10);
        EXPECT_TRUE(region.checkRegionCacheTTL(ts));
        EXPECT_EQ(region.ttl.load(), ts + 10);
    }

    Region::setRegionCacheTTLEnabled(false);
    Region::setRegionCacheTTL(/*base_sec=*/600, /*jitter_sec=*/60);
}

TEST_F(RegionCacheTest, OnRegionStaleSelectsPeerByStoreIDForCurrentRegions)
{
    auto original = makeMetaRegion(101, "", "", 1, 5, {{11, 1}, {12, 2}});
    auto pd_client = std::make_shared<RegionCachePDClient>(original, original.peers(0), std::initializer_list<uint64_t>{1, 2});
    auto cache = makeRegionCache(pd_client);

    Backoffer bo(GetMaxBackoff);
    auto loc = cache->locateKey(bo, "a");
    ASSERT_EQ(loc.region, RegionVerID(101, 1, 5));

    auto ctx = cache->getRPCContext(bo, loc.region, StoreType::TiKV, false, labelFilterInvalid);
    ASSERT_NE(ctx, nullptr);
    ASSERT_EQ(ctx->peer.id(), 11);
    ASSERT_EQ(ctx->peer.store_id(), 1);

    errorpb::EpochNotMatch stale_epoch;
    stale_epoch.add_current_regions()->CopyFrom(makeMetaRegion(102, "", "m", 1, 6, {{22, 2}, {21, 1}}));
    stale_epoch.add_current_regions()->CopyFrom(makeMetaRegion(103, "m", "", 1, 6, {{32, 2}, {31, 1}}));

    EXPECT_FALSE(cache->onRegionStale(bo, ctx, stale_epoch));

    auto left = cache->getRegionByID(bo, RegionVerID(102, 1, 6));
    auto right = cache->getRegionByID(bo, RegionVerID(103, 1, 6));
    ASSERT_NE(left, nullptr);
    ASSERT_NE(right, nullptr);
    EXPECT_EQ(left->leader_peer.id(), 21);
    EXPECT_EQ(left->leader_peer.store_id(), 1);
    EXPECT_EQ(right->leader_peer.id(), 31);
    EXPECT_EQ(right->leader_peer.store_id(), 1);
}

TEST_F(RegionCacheTest, OnRegionStaleBackoffsWhenClientEpochAheadOfTiKV)
{
    auto original = makeMetaRegion(101, "", "", 1, 5, {{11, 1}, {12, 2}});
    auto pd_client = std::make_shared<RegionCachePDClient>(original, original.peers(0), std::initializer_list<uint64_t>{1, 2});
    auto cache = makeRegionCache(pd_client);

    std::vector<BackoffEvent> events;
    Backoffer bo(GetMaxBackoff, /*total_sleep_=*/0, [&](const BackoffEvent & event) {
        events.push_back(event);
    });
    auto loc = cache->locateKey(bo, "a");
    auto ctx = cache->getRPCContext(bo, loc.region, StoreType::TiKV, false, labelFilterInvalid);
    ASSERT_NE(ctx, nullptr);

    errorpb::EpochNotMatch stale_epoch;
    stale_epoch.add_current_regions()->CopyFrom(makeMetaRegion(101, "", "", 1, 4, {{11, 1}, {12, 2}}));

    EXPECT_TRUE(cache->onRegionStale(bo, ctx, stale_epoch));

    ASSERT_EQ(events.size(), 1);
    EXPECT_EQ(events[0].type, boRegionMiss);
    EXPECT_EQ(events[0].error_code, RegionEpochNotMatch);

    auto cached = cache->getRegionByID(bo, loc.region);
    ASSERT_NE(cached, nullptr);
    EXPECT_EQ(cached->verID(), loc.region);
    EXPECT_EQ(cached->leader_peer.id(), 11);
    EXPECT_EQ(cached->leader_peer.store_id(), 1);
}

} // namespace pingcap::tests
