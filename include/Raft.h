#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ApplyMsg.h"
#include "RaftRpcUtil.h"
#include "Util.h"

class ThreadPool;
class Persister;

class Raft : public RaftRpcHandler
{
public:
    Raft(std::string ip, std::string port,
         std::unordered_map<int, const std::string> idToAddr,
         int id,
         std::shared_ptr<Persister> persister,
         std::shared_ptr<LockQueue<ApplyMsg>> applyQueue);
    ~Raft();

    Raft(const Raft &) = delete;
    Raft &operator=(const Raft &) = delete;

    void start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader); // 向Raft提交命令

    void OnRequestVote(const RaftRpc::RequestVoteArgs *request,
                       RaftRpc::RequestVoteReply *reply) override;
    void OnAppendEntriesStreamOn(std::unique_ptr<AppendEntriesResponder> responder,
                                 const std::string &peer) override;
    void OnAppendEntries(const RaftRpc::AppendEntriesArgs *request,
                         const std::string &peer) override;
    void OnAppendEntriesStreamClose(const std::string &peer) override;
    void OnInstallSnapshotStreamOn(std::unique_ptr<InstallSnapshotResponder> responder,
                                   const std::string &peer) override;
    void OnInstallSnapshotChunk(const RaftRpc::InstallSnapshotArgs *request,
                                const std::string &peer) override;
    void OnInstallSnapshotStreamClose(const std::string &peer) override;

    void updateCommitIndex();
    bool matchLog(int logIndex, int logTerm);
    void persist();
    bool whetherVoteFor(int index, int term);
    int getNewCommandIndex();
    void getPrevLogInfo(int server, int *preIndex, int *preTerm);
    void getState(int *term, bool *isLeader);
    int getLastLogIndex();
    int getLastLogTerm();
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    int getLogTermFromIndex(int logIndex);
    int getRaftStateSize();
    int getSlicesIndexFromLogIndex(int logIndex);
    std::vector<ApplyMsg> getApplyLogs();
    void pushMsgToKvServer(ApplyMsg msg);
    void readPersist(std::string value);
    std::string persistData();

    // for debug
    std::unordered_map<int, const std::string> getIdToAddr() const { return idToAddr_; }
    std::unordered_map<std::string, int> getAddrToId() const { return addrToId_; }
    std::unordered_map<int, std::shared_ptr<grpc::Channel>> getPeers() const { return peers_; }
    std::shared_ptr<Persister> getPersister() const { return persister_; }
    int getId() const { return id_; }

    int getCurrentTerm();
    int getVotedFor();
    std::vector<RaftRpc::LogEntry> getLogs();
    int getCommitIndex();
    int getLastApplied();
    std::unordered_map<int, int> getNextIndex();
    std::unordered_map<int, int> getMatchIndex();
    RaftRpc::RaftState getStatus();
    bool getStop() const { return stop_.load(); }

    std::shared_ptr<LockQueue<ApplyMsg>> getApplyQueue() const { return applyQueue_; }
    std::chrono::steady_clock::time_point getLastResetElectionTime();
    std::chrono::steady_clock::time_point LastResetHeartBeatTime();
    int getLastIncludeSnapshotIndex();
    int getLastIncludeSnapshotTerm();
    void setSnapshotCallback(const std::function<std::string()> &generator);

private:
    using Event = std::function<void()>;
    using Clock = std::chrono::steady_clock;

    struct StartResult
    {
        int index{-1};
        int term{-1};
        bool leader{false};
    };
    struct ServerSession
    {
        std::unique_ptr<AppendEntriesResponder> AEResponder;
        std::unique_ptr<InstallSnapshotResponder> ISResponder;
    };

    void postControl(Event event);
    void postCommand(Event event);
    void eventLoop();         // 事件循环 现在整个Raft状态机是无锁的
    void processEventBatch(); // 执行事件队列的事件
    void handleDeadlines();
    void resetElectionDeadline();
    void resetHeartbeatDeadline();
    void beginElection();
    void becomeFollower(int term);
    void becomeLeader();
    void scheduleAllReplication();
    void scheduleReplication(int server);
    void launchAppendEntries(int server, RaftRpc::AppendEntriesArgs request);
    void launchSnapshot(int server, RaftRpc::InstallSnapshotArgs request);
    void handleVoteReply(int server, int requestTerm, bool transportOk,
                         RaftRpc::RequestVoteReply reply);
    void handleAppendReply(int server, int requestTerm,
                           RaftRpc::AppendEntriesArgs request,
                           bool transportOk, RaftRpc::AppendEntriesReply reply);
    void handleSnapshotReply(int server, int requestTerm, bool transportOk,
                             RaftRpc::InstallSnapshotReply reply);
    void handleRequestVote(const RaftRpc::RequestVoteArgs &request,
                           RaftRpc::RequestVoteReply *reply);
    void handleAppendEntries(const RaftRpc::AppendEntriesArgs &request, int server);
    void handleInstallSnapshot(const RaftRpc::InstallSnapshotArgs &request, int server);
    void applyCommitted();
    void maybeTakeSnapshot();
    RaftRpc::AppendEntriesArgs buildAppendEntries(int server);
    const std::string getAddrById(int id);

    // 封装查询控制 getter可以直接据此查询
    template <typename T>
    T query(std::function<T()> reader)
    {
        if (std::this_thread::get_id() == eventThreadId_)
            return reader();
        auto promise = std::make_shared<std::promise<T>>();
        auto future = promise->get_future();
        postControl([promise, reader = std::move(reader)]() mutable
                    { promise->set_value(reader()); });
        return future.get();
    }

    std::string ip_;
    std::string port_;
    const std::unordered_map<int, const std::string> idToAddr_;
    std::unordered_map<std::string, int> addrToId_;
    std::unordered_map<int, std::shared_ptr<grpc::Channel>> peers_;
    std::unique_ptr<RaftServer> server_;
    std::unordered_map<int, std::unique_ptr<RaftClient>> clients_;
    std::unordered_map<int, ServerSession> streamServer_;
    std::unique_ptr<ThreadPool> threadPool_;

    std::mutex queueMutex_;
    std::condition_variable queueCv_;

    // 双队列 能保证control不会因为command过多而不执行
    std::deque<Event> controlQueue_; // 控制队列 高优先级
    std::deque<Event> commandQueue_; // 命令队列 低优先级
    std::thread eventThread_;
    std::thread::id eventThreadId_;
    bool eventLoopStopping_{false}; // 事件队列关闭标志
    std::atomic_bool stop_{false};

    std::shared_ptr<Persister> persister_;
    std::shared_ptr<LockQueue<ApplyMsg>> applyQueue_;
    std::function<std::string()> genSnapshotCallback_; // 上层的回调 用于生成快照

    int id_;
    int currentTerm_{0};
    int votedFor_{-1};
    std::vector<RaftRpc::LogEntry> logs_;
    int commitIndex_{0};
    int lastApplied_{0};
    std::unordered_map<int, int> nextIndex_;
    std::unordered_map<int, int> matchIndex_;
    // 每个follower一个worker 这样可以降低rpc数量
    std::unordered_map<int, bool> replicationInFlight_; // 与各个follower是否有worker
    std::unordered_map<int, bool> replicationPending_; // 各个follower是否有事件pending
    RaftRpc::RaftState status_{RaftRpc::RAFT_FOLLOWER};
    int votesGranted_{0};
    int electionTerm_{0};

    Clock::time_point lastResetElectionTime_;
    Clock::time_point lastResetHeartBeatTime_;
    Clock::time_point electionDeadline_;
    Clock::time_point heartbeatDeadline_;
    Clock::time_point snapshotDeadline_; // snapshot超时时间 通过轮询检查内存日志数量来决定是否生成快照

    int lastIncludeSnapshotIndex_{0};
    int lastIncludeSnapshotTerm_{0};
};
