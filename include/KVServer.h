#include <grpcpp/grpcpp.h>

#include "KVServer.grpc.pb.h"
#include "KVServer.pb.h"
#include "Raft.h"
#include "SkipLists.h"

// 共享、可失效的句柄
// Server和Reactor的中间层，封装线程安全的操作 解耦双方的依赖关系
struct RpcCompletion
{
    std::mutex mutex;
    bool done{false};
    bool cancelled{false};

    ServerRpc::PutAppendReply *reply{nullptr};
    grpc::ServerUnaryReactor *reactor{nullptr};

    void finish(const std::string &err)
    {
        grpc::ServerUnaryReactor *r = nullptr;
        // 在调用finish时，最好不要带自身的锁
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (done || cancelled || reactor == nullptr || reply == nullptr)
                return;
            done = true;
            reply->set_err(err);
            r = reactor;
        }
        r->Finish(grpc::Status::OK);
    }
    void cancel()
    {
        std::lock_guard<std::mutex> lock(mutex);
        cancelled = true;
        reply = nullptr;
        reactor = nullptr;
    }
};

class PutAppendReactor;
class GetReactor;
struct RequestKey
{
    std::string clientId;
    int requestId;
    // should define as const
    // 凡是类内重载比较运算如== >=等，就要为const
    bool operator==(const RequestKey &other) const
    {
        return this->clientId == other.clientId && this->requestId == other.requestId;
    }
};
struct PendingRequest
{
    int logIndex;
    int logTerm;
    std::shared_ptr<RpcCompletion> completion;
};
struct ApplyResult
{
    std::string clientId;
    int requestId;
    std::string replyMsg;
    std::string getReply;
};
namespace std
{
    template <>
    struct hash<RequestKey>
    {
        std::size_t operator()(const RequestKey &k) const
        {
            std::size_t h1 = std::hash<std::string>{}(k.clientId);
            std::size_t h2 = std::hash<int>{}(k.requestId);
            return h1 ^ (h2 << 1);
        }
    };
};

// skiplist KV server
/*
    上层来的请求决定要做什么，而raft决定对应的操作什么时候可以做，所以这里就天然有一个同步关系
    需要维护：请求来了后，需要由Raft分发、确定后再推到applyqueue里，此时才能操作db
*/
class KVServer : public ServerRpc::KVServerRpc::CallbackService
{
public:
    KVServer(std::string ServerIp, std::string ServerPort,
             std::string RaftIp, std::string RaftPort,
             std::unordered_map<int, const std::string> idToAddr,
             int id);
    ~KVServer();

    // sync api
    grpc::ServerUnaryReactor *Get(grpc::CallbackServerContext * /*context*/, const ServerRpc::GetArgs * /*request*/, ServerRpc::GetReply * /*reply*/) override;
    grpc::ServerUnaryReactor *PutAppend(grpc::CallbackServerContext * /*context*/, const ServerRpc::PutAppendArgs * /*request*/, ServerRpc::PutAppendReply * /*reply*/) override;

    void start();    // create server
    void wait();     // block and listening for request
    void shutDown(); // shutdown grpc server

    json debugDumpState();
    Raft* getRaft() const { return raft_.get(); }

private:
    class KVStateMachine
    {
        using SnapshotData = std::string;

    public:
        KVStateMachine()
            : kv_(maxLevel, dumpPath, delimiter)
        {
        }
        ApplyResult apply(const Op &op);
        // TODO:
        std::optional<std::string> get(const std::string &key) const;

        std::string snapshot();
        void restore(const json &);

    private:
        struct ClientRecord
        {
            int lastRequestId;
            std::string lastReplyMsg;
        };
        SkipList<std::string, std::string> kv_;
        std::unordered_map<std::string, ClientRecord> dedup_; // clientId->Record deduplication
        friend class KVServer;
    };

    template <class T>
    std::optional<T> queryStateMachine(const std::function<T()> &reader)
    {
        if (std::this_thread::get_id() == applyThreadId_)
            return reader();
        std::promise<T> expectedPromise;
        std::future<T> expectFuture = expectedPromise.get_future();
        bool success = postQuery([this, &expectedPromise, reader = std::move(reader)]()
                                 { expectedPromise.set_value(reader()); });
        if (success)
        {
            return expectFuture.get();
        }
        else
            return std::nullopt;
    }

    void applyWorkerLoop();
    bool postQuery(const std::function<void()> &);

    void completePending(ApplyMsg, ApplyResult);
    void cancelPending(const RequestKey &, std::shared_ptr<RpcCompletion>);
    void registerPending(int index, int term, std::string clientId, int requestid, std::shared_ptr<RpcCompletion>);
    void maybeSnapshot(int); // 根据Raft是否需要生成快照来生成快照

    static Op buildOp(const ServerRpc::PutAppendArgs &request);
    static Op parseCommand(std::string command); // parse from ApplyMsg's command string to Op

    std::unique_ptr<grpc::Server> server_;
    bool closed_{false};

    using StateTask = std::function<void()>;
    std::shared_ptr<LockQueue<ApplyMsg>> applyQue_;           // for apply
    std::shared_ptr<LockQueue<StateTask>> stateMachineQueue_; // for query
    std::mutex stateMachineMutex_;

    std::shared_ptr<Raft> raft_;
    std::shared_ptr<Persister> persister_;
    int id_;
    std::string ServerIp_;
    std::string ServerPort_;
    std::string RaftIp_;
    std::string RaftPort_;
    std::unordered_map<int, const std::string> idToAddr_;

    std::shared_ptr<std::thread> applyThread_;
    std::atomic_bool stopping_{false};
    std::thread::id applyThreadId_;
    int lastApplied_;

    std::mutex pendingMutex_;
    std::unordered_map<RequestKey, PendingRequest> pendingRequests_;
    KVStateMachine stateMachine_;

    friend class GetReactor;
    friend class PutAppendReactor;
};