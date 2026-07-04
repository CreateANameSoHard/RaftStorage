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
        applyQue1_ = std::make_shared<LockQueue<ApplyMsg>>();
        applyQue2_ = std::make_shared<LockQueue<ApplyMsg>>();
        applyQue3_ = std::make_shared<LockQueue<ApplyMsg>>();

        node1_ = std::make_unique<Raft>("127.0.0.1", "9000", idToAddr_, 1, persister1_, applyQue1_);
        node2_ = std::make_unique<Raft>("127.0.0.1", "9001", idToAddr_, 2, persister2_, applyQue2_);
        node3_ = std::make_unique<Raft>("127.0.0.1", "9002", idToAddr_, 3, persister3_, applyQue3_);
        node1_->setSnapshotCallback(std::bind(&RaftTest::genSnapshot, this));
        node2_->setSnapshotCallback(std::bind(&RaftTest::genSnapshot, this));
        node3_->setSnapshotCallback(std::bind(&RaftTest::genSnapshot, this));

        idToNode_[1] = node1_.get();
        idToNode_[2] = node2_.get();
        idToNode_[3] = node3_.get();

        nodeToId_[node1_.get()] = 1;
        nodeToId_[node2_.get()] = 2;
        nodeToId_[node3_.get()] = 3;
    }
    void TearDown() override
    {
        node1_.reset();
        node2_.reset();
        node3_.reset();

        applyQue1_.reset();
        applyQue2_.reset();
        applyQue3_.reset();
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

    void ruin(int id)
    {
        switch (id)
        {
        case 1:
            node1_.reset();
            idToNode_[1] = nullptr;
            break;
        case 2:
            node2_.reset();
            idToNode_[2] = nullptr;
            break;
        case 3:
            node3_.reset();
            idToNode_[3] = nullptr;
            break;
        }
    }

    void recover(int id)
    {
        switch (id)
        {
        case 1:
            node1_ = std::make_unique<Raft>("127.0.0.1", "9000", idToAddr_, 1, persister1_, applyQue1_);
            idToNode_[1] = node1_.get();
            break;
        case 2:
            node2_ = std::make_unique<Raft>("127.0.0.1", "9001", idToAddr_, 2, persister2_, applyQue2_);
            idToNode_[2] = node2_.get();
            break;
        case 3:
            node3_ = std::make_unique<Raft>("127.0.0.1", "9002", idToAddr_, 3, persister3_, applyQue3_);
            idToNode_[3] = node3_.get();
            break;
        }
    }

    std::vector<Raft *> getValidNodes()
    {
        std::vector<Raft *> nodes;
        for (auto &pair : idToNode_)
        {
            if (pair.second != nullptr)
                nodes.push_back(pair.second);
        }
        return nodes;
    }

    std::vector<int> getNonLeaderId()
    {
        std::vector<int> followers;
        for (int i = 1; i <= idToAddr_.size(); i++)
        {
            if (idToNode_[i] && idToNode_[i]->getStatus() != RaftRpc::RAFT_LEADER)
                followers.push_back(i);
        }
        return followers;
    }

    // 上层状态机生成快照
    std::string genSnapshot()
    {
        json j = json::array();
        // 这里做个模拟 实际应该是把上层状态机的元素序列化
        for (int i = 0; i < SnapshotThreshold; i++)
        {
            j.emplace(json{{"hello", "world"}});
        }
        return j.dump(4);
    }
    // 上层状态机应用快照
    void applySnapshot(const std::string &v)
    {
        bool flag;
        json j = json::parse(v);
        ASSERT_EQ(j.size(), SnapshotThreshold);
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

    std::unordered_map<int, Raft *> idToNode_;
    std::unordered_map<Raft *, int> nodeToId_;
};

TEST_F(RaftTest, DISABLED_ConstructAndDeconstructTest)
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

TEST_F(RaftTest, DISABLED_ElectionTest)
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

TEST_F(RaftTest, DISABLED_LogReplicationTest)
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
}

TEST_F(RaftTest, LeaderFailoverTest) {
    std::vector<Raft *> nodes = getValidNodes();
    int oldLeaderId = WaitForLeader(nodes, 5000);
    ASSERT_NE(oldLeaderId, -1);

    ruin(oldLeaderId);
    nodes = getValidNodes();  // 更新列表

    int newLeaderId = WaitForLeader(nodes, 5000);
    EXPECT_NE(newLeaderId, -1);
    EXPECT_NE(newLeaderId, oldLeaderId);
}

TEST_F(RaftTest, SnapshotPersistAndReadTest)
{
    std::vector<Raft *> nodes = getValidNodes();
    int oldleaderId = WaitForLeader(nodes, 5000);
    ASSERT_NE(oldleaderId, -1);

    ruin(oldleaderId);

    nodes = getValidNodes();
    int newleaderId = WaitForLeader(nodes, 5000);
    Raft *leader = nullptr;
    leader = idToNode_[newleaderId];
    ASSERT_NE(leader, nullptr);

    for (int i = 0; i < SnapshotThreshold; i++)
    {
        Op command;
        command.operation = "hello";
        int newLogIndex, newLogTerm;
        bool isLeader;
        leader->start(command, &newLogIndex, &newLogTerm, &isLeader);
        EXPECT_TRUE(isLeader);
    }
    // wait for persist snapshot
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::vector<int> follower = getNonLeaderId();
    ASSERT_EQ(follower.size(), 1);
    int normalId = -1;
    for (int i = 0; i < follower.size(); i++)
    {
        if (idToNode_[follower[i]]->getId() != oldleaderId)
            normalId = idToNode_[follower[i]]->getId();
    }
    ASSERT_NE(normalId, -1);
    EXPECT_EQ(idToNode_[normalId]->getCommitIndex(), idToNode_[newleaderId]->getCommitIndex());
    EXPECT_EQ(idToNode_[normalId]->getLastIncludeSnapshotIndex(), idToNode_[newleaderId]->getLastIncludeSnapshotIndex());

    recover(oldleaderId);
    // wait for installsnapshot and apply log
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    ApplyMsg msg;
    bool hasSnapshot = false;
    while (applyQue1_->timeoutPop(100, &msg))
    {
        if (msg.SnapshotValid_)
        {
            hasSnapshot = true;
            // 验证快照数据
            json snapData = msg.Snapshot_;
            EXPECT_EQ(snapData.size(), SnapshotThreshold);
            break;
        }
    }
    EXPECT_TRUE(hasSnapshot) << "No snapshot received by node1";
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}