#include <gtest/gtest.h>

#include "../include/Persister.h"
#include "../include/Raft.h"

class RaftTest : public testing::Test
{
protected:
    void SetUp() override
    {
        /*
            Raft(std::string ip, std::string port, std::unordered_map<int, const std::string> idToAddr, int id, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyQueue)
        */
        persister1_ = std::make_shared<Persister>(1);
        persister2_ = std::make_shared<Persister>(2);
        persister3_ = std::make_shared<Persister>(3);
        node1_ = std::make_unique<Raft>("127.0.0.1", "9000", idToAddr_, 1, persister1_, applyQue1_);
        node2_ = std::make_unique<Raft>("127.0.0.1", "9001", idToAddr_, 2, persister2_, applyQue2_);
        node3_ = std::make_unique<Raft>("127.0.0.1", "9002", idToAddr_, 3, persister3_, applyQue3_);
    }
    void TearDown() override
    {
        node1_.reset();
        node2_.reset();
        node3_.reset();
    }

    int WaitForLeader(const std::vector<Raft *> &nodes, int timeout_ms)
    {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - start)
                   .count() < timeout_ms)
        {
            int leaderCount = 0;
            int leaderId = -1;
            for (auto *node : nodes)
            {
                int term;
                bool isLeader;
                node->getState(&term, &isLeader);
                if (isLeader)
                {
                    leaderCount++;
                    leaderId = node->getId();
                }
            }
            if (leaderCount == 1)
                return leaderId;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return -1; // 超时
    }

    bool AllLogsMatch(const std::vector<Raft *> &nodes)
    {
        if (nodes.empty())
            return true;
        auto firstLogs = nodes[0]->getLogs();
        for (size_t i = 1; i < nodes.size(); ++i)
        {
            auto logs = nodes[i]->getLogs();
            if (logs.size() != firstLogs.size())
                return false;
            for (size_t j = 0; j < logs.size(); ++j)
            {
                if (logs[j].logindex() != firstLogs[j].logindex() ||
                    logs[j].logterm() != firstLogs[j].logterm() ||
                    logs[j].command() != firstLogs[j].command())
                {
                    return false;
                }
            }
        }
        return true;
    }

    std::unique_ptr<Raft> node1_;
    std::shared_ptr<Persister> persister1_;
    std::shared_ptr<LockQueue<ApplyMsg>> applyQue1_;

    std::unique_ptr<Raft> node2_;
    std::shared_ptr<Persister> persister2_;
    std::shared_ptr<LockQueue<ApplyMsg>> applyQue2_;

    std::unique_ptr<Raft> node3_;
    std::shared_ptr<Persister> persister3_;
    std::shared_ptr<LockQueue<ApplyMsg>> applyQue3_;

    std::unordered_map<int, const std::string> idToAddr_{
        {1, "127.0.0.1:9000"},
        {2, "127.0.0.1:9001"},
        {3, "127.0.0.1:9002"}};
};

TEST_F(RaftTest, ConstructAndDeconstructTest)
{
    ASSERT_TRUE(node1_->getApplierTickerThread() != nullptr);
    ASSERT_TRUE(node2_->getApplierTickerThread() != nullptr);
    ASSERT_TRUE(node3_->getApplierTickerThread() != nullptr);

    ASSERT_TRUE(node1_->getElectionTimeOutTickerThread() != nullptr);
    ASSERT_TRUE(node2_->getElectionTimeOutTickerThread() != nullptr);
    ASSERT_TRUE(node3_->getElectionTimeOutTickerThread() != nullptr);

    ASSERT_TRUE(node1_->getLeaderHeartBeatTickerThread() != nullptr);
    ASSERT_TRUE(node2_->getLeaderHeartBeatTickerThread() != nullptr);
    ASSERT_TRUE(node3_->getLeaderHeartBeatTickerThread() != nullptr);
}

TEST_F(RaftTest, ElectionTest)
{
    std::vector<Raft *> nodes = {node1_.get(), node2_.get(), node3_.get()};
    int leaderId = WaitForLeader(nodes, 5000);
    ASSERT_NE(leaderId, -1) << "No leader elected within timeout";

    int leaderCount = 0;
    for (auto *node : nodes)
    {
        int term;
        bool isLeader;
        node->getState(&term, &isLeader);
        if (isLeader)
            leaderCount++;
    }
    EXPECT_EQ(leaderCount, 1) << "More than one leader exists";
}

TEST_F(RaftTest, LogReplicationTest)
{
    std::vector<Raft *> nodes = {node1_.get(), node2_.get(), node3_.get()};
    int leaderId = WaitForLeader(nodes, 5000);
    ASSERT_NE(leaderId, -1);

    Raft *leader = nullptr;
    for (auto *node : nodes)
    {
        if (node->getId() == leaderId)
        {
            leader = node;
            break;
        }
    }
    ASSERT_NE(leader, nullptr);

    // 提交一条命令
    Op command;
    command.operation = "hello";
    int newLogIndex, newLogTerm;
    bool isLeader;
    leader->start(command, &newLogIndex, &newLogTerm, &isLeader);
    EXPECT_TRUE(isLeader);

    // 等待提交完成
    auto start = std::chrono::steady_clock::now();
    bool committed = false;
    while (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
               .count() < 3000)
    {
        if (leader->getCommitIndex() >= newLogIndex)
        {
            committed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(committed) << "Log not committed in time";

    // 检查所有节点日志一致
    EXPECT_TRUE(AllLogsMatch(nodes));
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}