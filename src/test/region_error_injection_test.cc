#include <pingcap/Exception.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/pd/MockPDClient.h>

#include <vector>

#include "test_helper.h"

namespace pingcap::tests
{
using namespace pingcap;
using namespace pingcap::kv;

namespace
{

class RegionErrorInjectionPDClient : public pd::MockPDClient
{
public:
    RegionErrorInjectionPDClient()
    {
        region_.set_id(101);
        region_.set_start_key("");
        region_.set_end_key("");
        region_.mutable_region_epoch()->set_conf_ver(7);
        region_.mutable_region_epoch()->set_version(9);

        leader_.set_id(201);
        leader_.set_store_id(301);
        region_.add_peers()->CopyFrom(leader_);

        store_.set_id(leader_.store_id());
        store_.set_address("127.0.0.1:65000");
        store_.set_peer_address("127.0.0.1:65001");
        store_.set_state(metapb::StoreState::Up);
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
        if (store_id != store_.id())
        {
            throw Exception("unexpected store id", StoreNotReady);
        }
        return store_;
    }

    std::vector<metapb::Store> getAllStores(bool) override { return {store_}; }

    RegionVerID regionID() const
    {
        return RegionVerID{
            region_.id(),
            region_.region_epoch().conf_ver(),
            region_.region_epoch().version(),
        };
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
    metapb::Store store_;
};

struct RegionErrorInjectionCluster
{
    ClusterPtr cluster;
    RegionVerID region_id;
};

RegionErrorInjectionCluster makeRegionErrorInjectionCluster()
{
    auto pd_client = std::make_shared<RegionErrorInjectionPDClient>();
    auto cluster = std::make_unique<Cluster>();

    cluster->rpc_client->stop();
    cluster->mpp_prober->stop();
    cluster->thread_pool->stop();

    ClusterConfig config;
    config.tiflash_engine_key = "engine";
    config.tiflash_engine_value = "tiflash";
    cluster->pd_client = pd_client;
    cluster->region_cache = std::make_unique<RegionCache>(pd_client, config);
    cluster->rpc_client = std::make_unique<RpcClient>(pd_client, config);
    cluster->api_version = kvrpcpb::APIVersion::V1;

    return {std::move(cluster), pd_client->regionID()};
}

KeyLocation loadTestRegion(Cluster * cluster)
{
    Backoffer bo = cluster->newBackoffer(GetMaxBackoff);
    return cluster->region_cache->locateKey(bo, "k");
}

errorpb::Error makeEpochNotMatchError()
{
    errorpb::Error err;
    err.mutable_epoch_not_match();
    return err;
}

} // namespace

TEST(RegionErrorInjectionTest, RegionMissInjectionEmitsBackoffBeforeUnaryRpc)
{
    auto test_cluster = makeRegionErrorInjectionCluster();
    auto location = loadTestRegion(test_cluster.cluster.get());
    ASSERT_EQ(location.region, test_cluster.region_id);

    int injector_calls = 0;
    test_cluster.cluster->setRegionErrorInjector([&](const RegionErrorInjectionContext & ctx) {
        injector_calls++;
        EXPECT_STREQ(ctx.rpc_name, "KvGet Failed");
        EXPECT_EQ(ctx.region_id, location.region);
        EXPECT_EQ(ctx.store_type, StoreType::TiKV);
        EXPECT_EQ(ctx.store_id, 301);
        EXPECT_EQ(ctx.store_addr, "127.0.0.1:65000");
        EXPECT_FALSE(ctx.is_stream);
        return RegionErrorInjection::regionMiss("injected region miss");
    });

    std::vector<BackoffEvent> events;
    Backoffer bo(/*max_sleep_=*/1, /*total_sleep_=*/0, [&](const BackoffEvent & event) {
        events.push_back(event);
    });
    RegionClient client(test_cluster.cluster.get(), location.region);
    kvrpcpb::GetRequest req;
    req.set_key("k");
    req.set_version(1);
    kvrpcpb::GetResponse resp;

    try
    {
        client.sendReqToRegion<RPC_NAME(KvGet)>(bo, req, &resp);
        FAIL() << "expected injected region miss to trip the backoff limit before RPC";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), RegionUnavailable);
        EXPECT_EQ(e.message(), "injected region miss");
    }

    EXPECT_EQ(injector_calls, 1);
    ASSERT_EQ(events.size(), 1);
    EXPECT_EQ(events[0].type, boRegionMiss);
    EXPECT_EQ(events[0].error_code, RegionUnavailable);
    EXPECT_EQ(events[0].error_message, "injected region miss");
    EXPECT_TRUE(events[0].max_sleep_exceeded);
}

TEST(RegionErrorInjectionTest, StoreUnavailableInjectionEmitsStoreNotReadyBackoffBeforeUnaryRpc)
{
    auto test_cluster = makeRegionErrorInjectionCluster();
    auto location = loadTestRegion(test_cluster.cluster.get());

    int injector_calls = 0;
    test_cluster.cluster->setRegionErrorInjector([&](const RegionErrorInjectionContext & ctx) {
        injector_calls++;
        EXPECT_STREQ(ctx.rpc_name, "KvGet Failed");
        EXPECT_EQ(ctx.region_id, location.region);
        EXPECT_EQ(ctx.store_id, 301);
        EXPECT_FALSE(ctx.is_stream);
        return RegionErrorInjection::storeUnavailable("injected unavailable store");
    });

    std::vector<BackoffEvent> events;
    Backoffer bo(/*max_sleep_=*/1, /*total_sleep_=*/0, [&](const BackoffEvent & event) {
        events.push_back(event);
    });
    RegionClient client(test_cluster.cluster.get(), location.region);
    kvrpcpb::GetRequest req;
    req.set_key("k");
    req.set_version(1);
    kvrpcpb::GetResponse resp;

    try
    {
        client.sendReqToRegion<RPC_NAME(KvGet)>(bo, req, &resp);
        FAIL() << "expected injected unavailable store to trip the backoff limit before RPC";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), StoreNotReady);
        EXPECT_EQ(e.message(), "injected unavailable store");
    }

    EXPECT_EQ(injector_calls, 1);
    ASSERT_EQ(events.size(), 1);
    EXPECT_EQ(events[0].type, boRegionMiss);
    EXPECT_EQ(events[0].error_code, StoreNotReady);
    EXPECT_EQ(events[0].error_message, "injected unavailable store");
    EXPECT_TRUE(events[0].max_sleep_exceeded);
}

TEST(RegionErrorInjectionTest, EpochNotMatchInjectionUsesRegionErrorPathBeforeUnaryRpc)
{
    auto test_cluster = makeRegionErrorInjectionCluster();
    auto location = loadTestRegion(test_cluster.cluster.get());

    int injector_calls = 0;
    test_cluster.cluster->setRegionErrorInjector([&](const RegionErrorInjectionContext & ctx) {
        injector_calls++;
        EXPECT_STREQ(ctx.rpc_name, "KvGet Failed");
        EXPECT_EQ(ctx.region_id, location.region);
        EXPECT_EQ(ctx.store_id, 301);
        EXPECT_FALSE(ctx.is_stream);
        return RegionErrorInjection::regionError(makeEpochNotMatchError());
    });

    std::vector<BackoffEvent> events;
    Backoffer bo(GetMaxBackoff, /*total_sleep_=*/0, [&](const BackoffEvent & event) {
        events.push_back(event);
    });
    RegionClient client(test_cluster.cluster.get(), location.region);
    kvrpcpb::GetRequest req;
    req.set_key("k");
    req.set_version(1);
    kvrpcpb::GetResponse resp;

    try
    {
        client.sendReqToRegion<RPC_NAME(KvGet)>(bo, req, &resp);
        FAIL() << "expected injected epoch-not-match to throw before RPC";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), RegionEpochNotMatch);
    }

    EXPECT_EQ(injector_calls, 1);
    EXPECT_TRUE(events.empty());
}

} // namespace pingcap::tests
