#include <memory>
#include <algorithm>

#include "../include/KVServer.h"
#include "../include/Persister.h"

namespace
{
    const std::string WrongLeaderMsg = "Wrong Leader";
    auto WrongLeaderStatus = grpc::Status(grpc::StatusCode::DO_NOT_USE, WrongLeaderMsg);
    const std::string TimeOutMsg = "TimeOut";
    auto TimeOutStatus = grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, TimeOutMsg);
    const std::string SuccessMsg = "Success";
    auto SuccessStatus = grpc::Status(grpc::StatusCode::OK, SuccessMsg);
    const std::string ExecuteErrorMsg = "ExecuteError";
    auto ExecuteErrorStatus = grpc::Status(grpc::StatusCode::UNKNOWN, ExecuteErrorMsg);

    std::string GET = "GET"; // select
    std::string PUT = "PUT"; // update or insert
    std::string DEL = "DEL"; // delete

    std::vector<std::string> operations = {GET, PUT, DEL};
};

ApplyResult KVServer::KVStateMachine::apply(const Op &op)
{
    auto it = dedup_.find(op.clientId);
    // 去重
    if (it != dedup_.end() && op.requestId <= it->second.lastRequestId)
        return ApplyResult{op.clientId, op.requestId, it->second.lastReplyMsg};

    ApplyResult result;
    if (op.operation == PUT)
    {
        if (kv_.insert(op.key, op.value))
            result.replyMsg = SuccessMsg;
        else
            result.replyMsg = ExecuteErrorMsg;
    }
    else if (op.operation == DEL)
    {
        if (kv_.erase(op.key))
            result.replyMsg = SuccessMsg;
        else
            result.replyMsg = ExecuteErrorMsg;
    }
    // TODO:
    else if (op.operation == GET)
    {
    }
    result.clientId = op.clientId;
    result.requestId = op.requestId;
    dedup_[op.clientId] = {op.requestId, result.replyMsg};
    return result;
}

std::optional<std::string> KVServer::KVStateMachine::get(const std::string &key) const
{
    return "";
}

KVServer::KVStateMachine::SnapshotData KVServer::KVStateMachine::snapshot()
{
    json root;
    root["snapshot"] = std::move(kv_.dumpToJson());
    for (const auto &entry : dedup_)
    {
        auto clientId = entry.first;
        root["deduplication"][clientId] =
            {
                {"lastRequestId", entry.second.lastRequestId},
                {"lastReplyMsg", entry.second.lastReplyMsg}};
    }
    return root.dump(4);
}
void KVServer::KVStateMachine::restore(const json &root)
{
    kv_.loadFromJson(root["snapshot"]);
    if (root["deduplication"].is_array() && root["deduplication"].is_object())
    {
        dedup_.clear();
        for (const auto &entry : root["deduplication"].items())
        {
            auto record = entry.value();
            dedup_.emplace(entry.key(), KVStateMachine::ClientRecord{
                                            record["lastRequestId"].get<int>(),
                                            record["lastReplyMsg"].get<std::string>()});
        }
    }
}
// TODO:启动时需要读快照文件的deduplication字段并保存到stateMachine的dedup里
KVServer::KVServer(std::string ServerIp, std::string ServerPort,
                   std::string RaftIp, std::string RaftPort,
                   std::unordered_map<int, const std::string> idToAddr,
                   int id)
    : id_(id),
      ServerIp_(ServerIp),
      ServerPort_(ServerPort),
      RaftIp_(RaftIp),
      RaftPort_(RaftPort),
      idToAddr_(std::move(idToAddr))
{
    bool localFound = false;
    for (const auto &peer : idToAddr_)
    {
        if (peer.first == id_ && RaftIp_ + ":" + RaftPort_ == peer.second)
        {
            localFound = true;
            break;
        }
    }
    myAssert(localFound, "[KVServer] there's no localaddr in idToAddr");
    persister_ = std::make_shared<Persister>(id_);
    applyQue_ = std::make_shared<LockQueue<ApplyMsg>>();
    stateMachineQueue_ = std::make_shared<LockQueue<StateTask>>();
    raft_ = std::make_shared<Raft>(RaftIp_, RaftPort_, idToAddr_, id_, persister_, applyQue_);

    auto expectReady = std::make_shared<std::promise<void>>();
    auto future = expectReady->get_future();
    applyThread_ = std::make_shared<std::thread>(
        [this, expectReady]()
        {
            applyThreadId_ = std::this_thread::get_id();
            expectReady->set_value();
            applyWorkerLoop();
        });
    future.get();
}

KVServer::~KVServer()
{
    if (server_)
        shutDown();

    stopping_ = true;
    if (applyThread_->joinable())
        applyThread_->join();
    applyThread_.reset();
    raft_.reset();
}

void KVServer::start()
{
    if (!closed_ && !stopping_)
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(ServerIp_ + ":" + ServerPort_, grpc::InsecureServerCredentials());
        builder.RegisterService(this);
        server_ = std::move(builder.BuildAndStart());
        myAssert(server_ != nullptr, "[KVServer] failed to listen on " + ServerIp_ + ":" + ServerPort_);
    }
}
void KVServer::wait()
{
    if (server_)
    {
        server_->Wait();
    }
}

void KVServer::shutDown()
{
    if (closed_)
        return;
    closed_ = true;
    if (server_)
    {
        server_->Shutdown();
        server_.reset();
    }
}
json KVServer::debugDumpState()
{
    return queryStateMachine<json>(
        [this]()->json
        {
            return stateMachine_.kv_.dumpToJson();
        }
    );
}

class GetReactor : public grpc::ServerUnaryReactor
{
public:
    GetReactor(grpc::CallbackServerContext *context, const ServerRpc::GetArgs *request, ServerRpc::GetReply *reply, KVServer *server)
        : context_(context),
          request_(request),
          reply_(reply),
          server_(server)
    {
        if (server_->raft_->getStatus() == RaftRpc::RAFT_LEADER)
        {
            // TODO:readIndex
            Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "unimplemented get"));
        }
    }

private:
    void OnCancel() override
    {
        // do nothing
    }
    void OnDone() override
    {
        delete this;
    }
    grpc::CallbackServerContext *context_;
    const ServerRpc::GetArgs *request_;
    ServerRpc::GetReply *reply_;
    KVServer *server_; // only can use raw pointer.
};

// version 1: reactor的裸指针直接由PendingRequest持有，kvServer直接和reactor交互，容易出现use-after-free
// version 2: 添加一个中间层RpcCompletion。由它保证操作的有效性。reactor向它注册reply，kvServer向它注册和消费
class PutAppendReactor : public grpc::ServerUnaryReactor
{
public:
    PutAppendReactor(grpc::CallbackServerContext *context, const ServerRpc::PutAppendArgs *request, ServerRpc::PutAppendReply *reply, KVServer *server)
        : context_(context),
          request_(request),
          reply_(reply),
          server_(server),
          completion_(std::make_shared<RpcCompletion>())
    {
        Op op = KVServer::buildOp(*request_);
        key_ = {op.clientId, op.requestId};
        completion_->reactor = this;
        completion_->reply = reply_;
        Start(op);
    }

private:
    void Start(const Op &op)
    {
        int index, term;
        bool isLeader;
        server_->raft_->start(op, &index, &term, &isLeader);
        if (!isLeader)
        {
            reply_->set_err(WrongLeaderMsg);
            Finish(grpc::Status::OK);
            return;
        }
        server_->registerPending(index, term, op.clientId, op.requestId, completion_);
    }

    void OnCancel() override
    {
        server_->cancelPending(key_, completion_);
        completion_->cancel();
        DPrintf("[PutAppendReactor] peer cancel: %s", context_->peer().c_str());
    }
    void OnDone() override
    {
        server_->cancelPending(key_, completion_);
        completion_->cancel();
        delete this;
    }

    grpc::CallbackServerContext *context_;
    const ServerRpc::PutAppendArgs *request_;
    ServerRpc::PutAppendReply *reply_;
    KVServer *server_;
    std::shared_ptr<RpcCompletion> completion_;
    RequestKey key_;
};

void KVServer::applyWorkerLoop()
{
    int appliedSinceSnapshotCheck  = 0;
    while (!stopping_)
    {
        StateTask task;
        // drain query every turn
        while (stateMachineQueue_->tryPop(&task))
            task();

        ApplyMsg msg;
        if (!applyQue_->timeoutPop(100, &msg))
            continue;

        if (msg.CommandValid_ && !msg.SnapshotValid_)
        {
            Op op = parseCommand(msg.Command_);
            auto result = stateMachine_.apply(op);
            // 只有leader才会接收到rpc，即pendRequests才会有内容
            // 但不能直接用status判断leader然后completePending。因为状态是随时变化的
            completePending(msg, std::move(result));
            // TODO:不应该每apply一条日志就检查一次
            if(msg.CommandIndex_ - appliedSinceSnapshotCheck >= 50)
            {
                appliedSinceSnapshotCheck = msg.CommandIndex_;
                maybeSnapshot(msg.CommandIndex_);
            }
        }
        else if (msg.SnapshotValid_)
        {
            stateMachine_.restore(msg.Snapshot_);
        }
    }
}

bool KVServer::postQuery(const std::function<void()> &func)
{
    std::lock_guard<std::mutex> lock(stateMachineMutex_);
    if (stopping_)
        return false;
    stateMachineQueue_->push(func);
    return true;
}

void KVServer::completePending(ApplyMsg msg, ApplyResult result)
{
    if (result.clientId.empty() || !result.requestId)
        return;

    std::shared_ptr<RpcCompletion> completion;
    RequestKey key = {result.clientId, result.requestId};
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pendingRequests_.find(key);
        if (it == pendingRequests_.end())
            return;
        auto pending = it->second;
        if (msg.CommandIndex_ != pending.logIndex || msg.CommandTerm_ != pending.logTerm)
        {
            pendingRequests_.erase(it);
            return;
        }
        pendingRequests_.erase(it);
        completion = pending.completion;
    }
    // 不要在锁里finish completion->finish->PutReactor->OnDone/OnCancel->cancelPending->completion->cancel 然后死锁
    if (completion)
        completion->finish(result.replyMsg);
}

void KVServer::cancelPending(const RequestKey &key, std::shared_ptr<RpcCompletion> completion)
{
    if (!completion)
        return;
    // cancel需要注意是否有残留的pendingRequest
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pendingRequests_.find(key);
        if (it != pendingRequests_.end() && it->second.completion == completion)
        {
            pendingRequests_.erase(it);
        }
    }
    completion->cancel();
}

void KVServer::registerPending(int index, int term, std::string clientId, int requestid, std::shared_ptr<RpcCompletion> completion)
{
    RequestKey key{clientId, requestid};
    PendingRequest pend{index, term, completion};
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pendingRequests_.find(key);
        if (it == pendingRequests_.end())
        {
            pendingRequests_[key] = pend;
        }
        else
        {
            DPrintf("[registerPending] extra request");
        }
    }
}
// TODO:
void KVServer::maybeSnapshot(int commandIndex)
{
    lastApplied_ = commandIndex;
    if (raft_->needSnapshot(lastApplied_))
    {
        raft_->snapshot(lastApplied_, stateMachine_.snapshot());
    }
}

Op KVServer::buildOp(const ServerRpc::PutAppendArgs &request)
{
    return Op{request.op(), request.key(), request.value(), request.clientid(), request.requestid()};
}

Op KVServer::parseCommand(std::string command)
{
    json j = json::parse(command);
    Op result = {j.at("operation"), j.at("key"), j.at("value"), j.at("clientId"), j.at("requestId")};
    return result;
}

grpc::ServerUnaryReactor *KVServer::Get(grpc::CallbackServerContext *context, const ServerRpc::GetArgs *request, ServerRpc::GetReply *reply)
{
    return new GetReactor(context, request, reply, this);
}

grpc::ServerUnaryReactor *KVServer::PutAppend(grpc::CallbackServerContext *context, const ServerRpc::PutAppendArgs *request, ServerRpc::PutAppendReply *reply)
{
    return new PutAppendReactor(context, request, reply, this);
}
