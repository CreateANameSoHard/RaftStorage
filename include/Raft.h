#pragma once
#include <chrono>
#include <mutex>
#include <vector>
#include <memory>
#include <unordered_map>

#include "../include/RaftRpcUtil.h"
#include "../include/Util.h"
#include "../include/ApplyMsg.h"

class ThreadPool;
class Persister;

class Raft : public RaftRpcHandler
{
public:
    Raft(std::string, std::string, std::unordered_map<int, const std::string> idToAddr, int id, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyQueue);
    ~Raft();
    // 上层接口
    void start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

    void OnRequestVote(const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply) override;

    void OnAppendEntriesStreamOn(std::unique_ptr<AppendEntriesResponder> responder, const std::string &peer) override;
    void OnAppendEntries(const RaftRpc::AppendEntriesArgs *request, const std::string &peer) override;
    void OnAppendEntriesStreamClose(const std::string &peer) override;

    void OnInstallSnapshotStreamOn(std::unique_ptr<InstallSnapshotResponder> responder, const std::string &peer) override;
    void OnInstallSnapshotChunk(const RaftRpc::InstallSnapshotArgs *request, const std::string &peer) override;
    void OnInstallSnapshotStreamClose(const std::string &peer) override;

    void doHeartBeat(); // 发送心跳 only for leader
    void doElection();  // 发送选举

    void SendRequestVote(int server, std::shared_ptr<RaftRpc::RequestVoteArgs>, std::shared_ptr<RaftRpc::RequestVoteReply>, std::shared_ptr<std::atomic_int>); // 封装超时
    int SendAppendEntries(int server, std::shared_ptr<RaftRpc::AppendEntriesArgs>, std::shared_ptr<RaftRpc::AppendEntriesReply>);                              // 封装同步读写和超时

    void applierTicker();
    void electionTimeOutTicker(); // 定时器用于检测消息超时 如果超时了则发起选举
    void leaderHeartBeatTicker();
    void leaderSendSnapshot(int server); // 发送快照给落后follower

    void updateCommitIndex();                 // 统计peers的matchIndex 根据节点的提交信息来更新commitIndex 要注意延迟提交机制
    bool matchLog(int logIndex, int logTerm); // 匹配日志信息 即logIndex的term和logTerm是否一致 用于查看是否有心跳的日志
    void persist();                           // 持久化raft节点易失性状态 currentTerm log votedFor lastIncludeSnapshotIndex lastIncludeSnapshotTerm
    bool whetherVoteFor(int index, int term); // 判断index和term是否比当前节点更新 用于选举限制

    int getNewCommandIndex();                                     // 获取新命令所需的index
    void getPrevLogInfo(int server, int *preIndex, int *preTerm); // 获取对应id的preIndex和preTerm preIndex和preTerm为传出参数
    void getState(int *term, bool *isLeader);                     // 返回节点的currentTerm 和 状态
    int getLastLogIndex();                                        // 获取lastLogIndex
    int getLastLogTerm();                                         // 获取lastLogTerm
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    int getLogTermFromIndex(int logIndex); // logIndex为逻辑index 而不是logs的物理index
    int getRaftStateSize();
    int getSlicesIndexFromLogIndex(int logIndex); // logIndex和数组的index不是一个值 所以需要映射 lastIncludeSnapshotIndex的下一个日志视为0位置的日志

    std::vector<ApplyMsg> getApplyLogs(); // 获取即将提交给状态机的logs 即从lastApplied到lastLogIndex之间的日志内容
    void pushMsgToKvServer(ApplyMsg msg);
    void readPersist(std::string v); // 从string（序列化为string的raft状态值）里读取状态并赋值给this对象
    std::string persistData();

    //For Debug
    std::unordered_map<int, const std::string> getIdToAddr() const { return idToAddr_; }
    std::unordered_map<std::string, int> getAddrToId() const { return addrToId_; }
    std::unordered_map<int, std::shared_ptr<grpc::Channel>> getPeers() const { return peers_; }
    std::shared_ptr<Persister> getPersister() const { return persister_; }

    int getId() const { return id_; }
    int getCurrentTerm() const { return currentTerm_; }
    int getVotedFor() const { return votedFor_; }
    std::vector<RaftRpc::LogEntry> getLogs() const { return logs_; }

     int getCommitIndex() const { return commitIndex_; }
    int getLastApplied() const { return lastApplied_;}
    std::unordered_map<int,int> getNextIndex() const { return nextIndex_; }
    std::unordered_map<int,int> getMatchIndex() const { return matchIndex_; }

    RaftRpc::RaftState getStatus() const { return status_; }
    bool getStop() const { return stop_.load(); }
    std::thread* getLeaderHeartBeatTickerThread() const { return leaderHeartBeatTickerThread_.get(); }
    std::thread* getElectionTimeOutTickerThread() const { return electionTimeOutTickerThread_.get(); }
    std::thread* getApplierTickerThread() const { return applierTickerThread_.get(); }

    std::shared_ptr<LockQueue<ApplyMsg>> getApplyQueue() const { return applyQueue_; }

    std::chrono::steady_clock::time_point getLastResetElectionTime() const { return lastResetElectionTime_; }
    std::chrono::steady_clock::time_point LastResetHeartBeatTime() const { return lastResetHeartBeatTime_; }

    int getLastIncludeSnapshotIndex() const { return lastIncludeSnapshotIndex_; }
    int getLastIncludeSnapshotTerm() const { return lastIncludeSnapshotTerm_; }

private:
    // int getIdByAddr(const std::string addr);
    const std::string getAddrById(int id);

    std::unique_ptr<ThreadPool> threadPool_;

    struct ServerSession
    {
        std::unique_ptr<AppendEntriesResponder> AEResponder;
        std::unique_ptr<InstallSnapshotResponder> ISResponder;
    };
    std::string ip_;
    std::string port_;

    std::mutex mutex_;
    std::unordered_map<int, const std::string> idToAddr_;
    std::unordered_map<std::string, int> addrToId_;

    std::unordered_map<int, std::shared_ptr<grpc::Channel>> peers_;
    std::unique_ptr<RaftServer> server_;
    std::unordered_map<int, std::unique_ptr<RaftClient>> clients_;
    // 局部流 就不需要在这里注册了
    // std::unordered_map<int, Raft::FollowerSession> streamClient_;
    std::unordered_map<int, Raft::ServerSession> streamServer_;

    std::shared_ptr<Persister> persister_; // 持久化器

    // 需要持久化的四个节点状态
    int id_;                              // 节点编号
    int currentTerm_;                     // 节点已知的term 需要持久化
    int votedFor_;                        // 投票对象 需要持久化
    std::vector<RaftRpc::LogEntry> logs_; // 日志数组 需要持久化

    // 下列状态不需要持久化
    int commitIndex_;             // 节点的已提交索引 注意分辨leadercommit和commitIndex
    int lastApplied_;             // 最后一个提交给状态机的日志索引
    std::unordered_map<int,int> nextIndex_;  // nextIndex数组
    std::unordered_map<int,int> matchIndex_; // matchIndex数组

    RaftRpc::RaftState status_; // 节点状态
    std::atomic_bool stop_;
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