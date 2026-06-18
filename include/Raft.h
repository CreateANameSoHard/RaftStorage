#pragma once
#include <chrono>
#include <mutex>
#include <vector>
#include <memory>
#include <unordered_map>

#include "../include/RaftRpcUtil.h"
#include "../include/Util.h"
#include "../include/ApplyMsg.h"

class Persister;

class Raft : public RaftRpcHandler
{
public:
    Raft(std::vector<std::shared_ptr<grpc::Channel>> peers, int id, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyQueue);
    ~Raft();
    // 上层接口
    void start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

    void OnRequestVote(const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply) override;

    void OnAppendEntriesStreamOn(std::unique_ptr<AppendEntriesResponder> responder) override;
    void OnAppendEntries(const RaftRpc::AppendEntriesArgs *request) override;
    void OnAppendEntriesStreamClose() override;

    void OnInstallSnapshotStreamOn(std::unique_ptr<InstallSnapshotResponder> responder) override;
    void OnInstallSnapshotChunk(const RaftRpc::InstallSnapshotArgs *request) override;
    void OnInstallSnapshotStreamClose() override;

    void doHeartBeat(); // 发送心跳 only for leader
    void doElection();  // 发送选举

    void SendRequestVote(int server, std::shared_ptr<RaftRpc::RequestVoteArgs>, std::shared_ptr<RaftRpc::RequestVoteReply>, std::shared_ptr<int>); //封装超时
    void SendAppendEntries(int server, std::shared_ptr<RaftRpc::AppendEntriesArgs>, std::shared_ptr<RaftRpc::AppendEntriesReply>, std::shared_ptr<int>); //封装同步读写和超时

    void applierTicker();
    void electionTimeOutTicker(); // 定时器用于检测消息超时 如果超时了则发起选举
    void leaderHeartBeatTicker();
    void leaderSendSnapshot(int server); // 发送快照给落后follower

    void updateCommitIndex();
    bool matchLog(int logIndex, int logTerm); // 匹配日志信息
    void persist();
    void upToDate(int index, int term); // 更新follower的日志

    int getNewCommandIndex();
    void getPrevLogInfo(int server, int *preIndex, int *preTerm); // 获取对应id的preIndex和preTerm
    void getState(int *term, bool *isLeader);
    int getLastLogIndex();
    int getLastLogTerm();
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    int getLogTermFromIndex(int logIndex);
    int getRaftStateSize();
    int getSlicesIndexFromLogIndex(int logIndex);

    std::vector<ApplyMsg> getApplyLogs();
    void pushMsgToKvServer(ApplyMsg msg);
    void readPersist(std::string v);
    std::string persistData();

private:
    struct FollowerSession
    {
        std::unique_ptr<grpc::ClientContext> context;
        std::unique_ptr<AppendEntriesStream> stream;
    };

    std::mutex mutex_;
    std::vector<std::shared_ptr<grpc::Channel>> peers_; // TODO:优化连接池
    std::unique_ptr<RaftServer> server_;
    std::unordered_map<int, std::unique_ptr<RaftClient>> clients_;

    std::unordered_map<int, Raft::FollowerSession> appendEntriesClient_;
    std::unique_ptr<AppendEntriesResponder> appendEntriesResponder_; // 节点只需要和


    std::shared_ptr<Persister> persister_; // 持久化器

    int id_;                              // 节点编号
    int currentTerm_;                     // 节点已知的term 需要持久化
    int votedFor_;                        // 投票对象 需要持久化
    std::vector<RaftRpc::LogEntry> logs_; // 日志数组 需要持久化

    // 下列状态不需要持久化
    int commitIndex_;             // 节点的已提交索引 注意分辨leadercommit和commitIndex
    int lastApplied_;             // 最后一个提交给状态机的日志索引
    std::vector<int> nextIndex_;  // nextIndex数组
    std::vector<int> matchIndex_; // matchIndex数组

    RaftRpc::RaftState status_; // 节点状态
    std::unique_ptr<std::thread> leaderHeartBeatTickerThread_;
    std::unique_ptr<std::thread> electionTimeOutTickerThread_;
    std::unique_ptr<std::thread> applierTickerThread_;

    std::shared_ptr<LockQueue<ApplyMsg>> applyQueue_; // Server与Raft的通信接口

    std::chrono::steady_clock::time_point lastResetElectionTime_;  // 上次重置选举的时间点
    std::chrono::steady_clock::time_point lastResetHeartBeatTime_; // 上次重置心跳的时间点

    // 保存的快照信息
    int lastIncludeSnapshotIndex_;
    int lastIncludeSnapshotTerm_;
};