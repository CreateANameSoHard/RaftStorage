#include <gtest/gtest.h>
#include <algorithm>

#include "../include/KVServer.h"
#include "../include/SkipLists.h"

class TestClient
{
public:
    TestClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(ServerRpc::KVServerRpc::NewStub(channel))
    {
    }

    grpc::Status put(grpc::ClientContext *context, const ServerRpc::PutAppendArgs &request, ServerRpc::PutAppendReply *reply)
    {
        return stub_->PutAppend(context, request, reply);
    }
    grpc::Status get(grpc::ClientContext *context, const ServerRpc::GetArgs &request, ServerRpc::GetReply *reply)
    {
        return stub_->Get(context, request, reply);
    }

private:
    std::unique_ptr<ServerRpc::KVServerRpc::Stub> stub_;
};

class KVServerTest : public testing::Test
{
protected:
    void SetUp() override
    {
        node1_ = std::make_unique<KVServer>("127.0.0.1", "8500", "127.0.0.1", "9000", idToAddr_, 1);
        node2_ = std::make_unique<KVServer>("127.0.0.1", "8501", "127.0.0.1", "9001", idToAddr_, 2);
        node3_ = std::make_unique<KVServer>("127.0.0.1", "8502", "127.0.0.1", "9002", idToAddr_, 3);

        node1_->start();
        node2_->start();
        node3_->start();

        serverThreads_.emplace_back(std::make_shared<std::thread>([this]()
                                                                  { node1_->wait(); }));
        serverThreads_.emplace_back(std::make_shared<std::thread>([this]()
                                                                  { node2_->wait(); }));
        serverThreads_.emplace_back(std::make_shared<std::thread>([this]()
                                                                  { node3_->wait(); }));

        channel1_ = grpc::CreateChannel("127.0.0.1:8500", grpc::InsecureChannelCredentials());
        channel2_ = grpc::CreateChannel("127.0.0.1:8501", grpc::InsecureChannelCredentials());
        channel3_ = grpc::CreateChannel("127.0.0.1:8502", grpc::InsecureChannelCredentials());

        clients_[1] = std::make_unique<TestClient>(channel1_);
        clients_[2] = std::make_unique<TestClient>(channel2_);
        clients_[3] = std::make_unique<TestClient>(channel3_);

        idToNode_[1] = node1_.get();
        idToNode_[2] = node2_.get();
        idToNode_[3] = node3_.get();

        nodeToId_[node1_.get()] = 1;
        nodeToId_[node2_.get()] = 2;
        nodeToId_[node3_.get()] = 3;
    }
    void TearDown() override
    {
        node1_->shutDown();
        node2_->shutDown();
        node3_->shutDown();

        for (const auto &thread : serverThreads_)
        {
            if (thread->joinable())
                thread->join();
        }

        node1_.reset();
        node2_.reset();
        node3_.reset();
    }

    int waitForLeaders(int timeout)
    {
        leaderInfo info;
        bool success = waitHelper(
            [this, &info]() -> bool
            {
                info = getLeaders();
                if (info.leaderCnt == 1 && info.leaderId[0])
                    return true;
                else
                    return false;
            },
            timeout, 500);
        if (success)
            return info.leaderId[0];
        else
            return -1;
    }

    bool waitHelper(const std::function<bool()> &condition, int timeoutMs, int checkInterval = 100)
    {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() < timeoutMs)
        {
            if (condition())
                return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(checkInterval));
        }
        return condition();
    }

    std::vector<int> getFollowersId(int leaderId)
    {
        std::vector<int> result;
        if (leaderId > idToNode_.size())
            return result;
        for (const auto &entry : idToNode_)
        {
            int id = entry.first;
            if (id == leaderId)
                continue;
            else
                result.emplace_back(id);
        }
        return result;
    }

    bool checkConsistency(int leaderId,
                          const std::unordered_map<std::string, std::string> &expectedKV)
    {
        std::optional<json> leaderState = idToNode_[leaderId]->debugDumpState();
        if(!leaderState.has_value()) return false;

        for (const auto &entry : idToNode_)
        {
            json nodeState = entry.second->debugDumpState();
            if (nodeState != leaderState)
            {
                return false;
            }
        }

        json expectedJson = expectedKV;
        return leaderState == expectedJson;
    }

    struct leaderInfo
    {
        int leaderCnt{0};
        std::vector<int> leaderId;
    };
    KVServerTest::leaderInfo getLeaders()
    {
        leaderInfo info;
        for (const auto &entry : idToNode_)
        {
            if (entry.second->getRaft()->getStatus() == RaftRpc::RAFT_LEADER)
            {
                info.leaderId.emplace_back(entry.second->getRaft()->getId());
                info.leaderCnt++;
            }
        }
        return info;
    }

    ServerRpc::PutAppendArgs buildPutRequest(std::string key, std::string value)
    {
        ServerRpc::PutAppendArgs request;
        request.set_clientid(clientId);
        request.set_requestid(getRequestId());
        request.set_op("PUT");
        request.set_key(key);
        request.set_value(value);
        return request;
    }

    std::unique_ptr<KVServer> node1_;
    std::unique_ptr<KVServer> node2_;
    std::unique_ptr<KVServer> node3_;

    std::unordered_map<int, std::unique_ptr<TestClient>> clients_;
    std::shared_ptr<grpc::Channel> channel1_;
    std::shared_ptr<grpc::Channel> channel2_;
    std::shared_ptr<grpc::Channel> channel3_;

    std::vector<std::shared_ptr<std::thread>> serverThreads_;

    std::unordered_map<int, const std::string> idToAddr_{
        {1, "127.0.0.1:9000"},
        {2, "127.0.0.1:9001"},
        {3, "127.0.0.1:9002"}};
    std::unordered_map<int, KVServer *> idToNode_;
    std::unordered_map<KVServer *, int> nodeToId_;

    static int requestIdGenerator;
    static int getRequestId();
    static std::string clientId;
};

std::string KVServerTest::clientId = "1";
int KVServerTest::requestIdGenerator = 1;
int KVServerTest::getRequestId()
{
    return requestIdGenerator++;
}

TEST_F(KVServerTest, DISABLED_InitalLeaderElectionTest)
{
    auto leader = waitForLeaders(5000);
    ASSERT_NE(leader, -1);
}

TEST_F(KVServerTest, PutTest)
{
    auto leader = waitForLeaders(5000);
    ASSERT_NE(leader, -1);

    auto timeout = std::chrono::milliseconds(1000);

    ServerRpc::PutAppendArgs request = buildPutRequest("hello", "world");
    ServerRpc::PutAppendReply reply;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    auto status = clients_[leader]->put(&context, request, &reply);

    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(reply.err(), "Success");

    // wait for execute
    EXPECT_TRUE(waitHelper(
        [this, leader]() -> bool
        {
            return checkConsistency(leader, std::unordered_map<std::string, std::string>{{"hello", "world"}});
        },
        3000));
}

TEST_F(KVServerTest, WrongLeaderTest)
{
    int leader = waitForLeaders(5000);
    ASSERT_NE(leader, -1);
    std::vector<int> followers = getFollowersId(leader);
    ASSERT_EQ(followers.size(), idToNode_.size() - 1);

    auto timeout = std::chrono::milliseconds(1000);

    for (const auto &follower : followers)
    {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + timeout);
        ServerRpc::PutAppendArgs request = buildPutRequest("hello", "world");
        ServerRpc::PutAppendReply reply;
        auto status = clients_[follower]->put(&context, request, &reply);
        ASSERT_TRUE(status.ok());
        EXPECT_EQ(reply.err(), "Wrong Leader");
    }
}

TEST_F(KVServerTest, SequentialPutTest)
{
    int leader = waitForLeaders(5000);
    ASSERT_NE(leader, -1);

    int times = 10;
    int key = 65;
    int value = 1;
    for (int i = 0; i < times; i++)
    {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
        ServerRpc::PutAppendArgs request = buildPutRequest(std::to_string(key++), std::to_string(value++));
        ServerRpc::PutAppendReply reply;
        auto status = clients_[leader]->put(&context, request, &reply);

        ASSERT_TRUE(status.ok());
        EXPECT_EQ(reply.err(), "Success");
    }
    key = 65;
    value = 1;
    std::unordered_map<std::string, std::string> expectedKV;
    for (int i = 0; i < times; i++)
    {
        expectedKV.emplace(std::to_string(key++), std::to_string(value++));
    }

    EXPECT_TRUE(waitHelper(
        [this, leader, &expectedKV]() -> bool
        {
            return checkConsistency(leader, expectedKV);
        },
        3000));
}

TEST_F(KVServerTest, OverWritePutTest)
{
    int leader = waitForLeaders(5000);
    ASSERT_NE(leader, -1);

    grpc::ClientContext context1;
    context1.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
    ServerRpc::PutAppendArgs request1 = buildPutRequest("hello", "world");
    ServerRpc::PutAppendReply reply1;
    auto status1 = clients_[leader]->put(&context1, request1, &reply1);
    ASSERT_TRUE(status1.ok());

    grpc::ClientContext context2;
    context2.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
    ServerRpc::PutAppendArgs request2 = buildPutRequest("hello", "raft");
    ServerRpc::PutAppendReply reply2;
    auto status2 = clients_[leader]->put(&context2, request2, &reply2);
    ASSERT_TRUE(status2.ok());
    EXPECT_TRUE(waitHelper(
        [this, leader]() -> bool
        {
            return checkConsistency(leader, std::unordered_map<std::string, std::string>{{"hello", "raft"}});
        },
        3000));
}

TEST_F(KVServerTest, ManyPutTest)
{
    int leader = waitForLeaders(5000);
    ASSERT_NE(leader, -1);

    std::unordered_map<std::string, std::string> expected;
    int key = 1;
    int value = 100;

    int count = 500;
    for (int i = 0; i < count; i++)
    {
        grpc::ClientContext context1;
        context1.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(3000));
        expected.emplace(std::to_string(key), std::to_string(value));
        ServerRpc::PutAppendArgs request1 = buildPutRequest(std::to_string(key++), std::to_string(value++));
        ServerRpc::PutAppendReply reply1;
        auto status1 = clients_[leader]->put(&context1, request1, &reply1);
        ASSERT_TRUE(status1.ok());
        std::cout << "time " << i + 1 << "complete" << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_TRUE(waitHelper([this, leader, &expected]() -> bool
                           { return checkConsistency(leader, expected); }, 3000));
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}