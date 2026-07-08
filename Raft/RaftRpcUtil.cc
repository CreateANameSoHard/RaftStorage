#include <iostream>
#include <queue>

#include "../include/RaftRpcUtil.h"
#include "../include/Util.h"

RaftServer::RaftServer(std::string ip, std::string port, RaftRpcHandler *handler)
    : handler_(handler),
      closed_(false)
{
    std::string addr = ip + ":" + port;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = std::move(builder.BuildAndStart());
    DPrintf("[RaftServer Initialized] addr: %s", addr.c_str());
}

void RaftServer::run()
{
    closed_ = false;
    server_->Wait();
}
void RaftServer::shutdown()
{
    if (closed_)
        return;
    closed_ = true;
    server_->Shutdown();
    DPrintf("[RaftServer shotdown] server shutdown success");
}

class RequestVoteReactor : public grpc::ServerUnaryReactor
{
public:
    RequestVoteReactor(grpc::CallbackServerContext *context, const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply, RaftRpcHandler *handler)
        : context_(context),
          request_(request),
          reply_(reply),
          handler_(handler)
    {
        DPrintf("[start RequestVoteRpc]");
        // 上层需要能获得reply
        handler_->OnRequestVote(request_, reply_);
        Finish(grpc::Status::OK);
    }

private:
    void OnDone() override
    {
        std::cout << "[RequestVoteRpc Completed]" << std::endl;
        delete this;
    }
    void OnCancel() override
    {
        std::cerr << "[RequestVoteRpc Canceled] peer: " << context_->peer() << std::endl;
    }
    grpc::CallbackServerContext *context_;
    const RaftRpc::RequestVoteArgs *request_;
    RaftRpc::RequestVoteReply *reply_;
    RaftRpcHandler *handler_;
};

grpc::ServerUnaryReactor *RaftServer::RequestVote(grpc::CallbackServerContext *context, const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply)
{
    return new RequestVoteReactor(context, request, reply, handler_);
}

class PreVoteReactor : public grpc::ServerUnaryReactor
{
public:
    PreVoteReactor(grpc::CallbackServerContext *context, const RaftRpc::PreVoteArgs *request, RaftRpc::PreVoteReply*reply, RaftRpcHandler *handler)
        : context_(context),
          request_(request),
          reply_(reply),
          handler_(handler)
    {
        DPrintf("[start PreVoteRpc]");
        // 上层需要能获得reply
        handler_->OnPreVote(request_, reply_);
        Finish(grpc::Status::OK);
    }

private:
    void OnDone() override
    {
        std::cout << "[PreVoteRpc Completed]" << std::endl;
        delete this;
    }
    void OnCancel() override
    {
        std::cerr << "[PreVoteRpc Canceled] peer: " << context_->peer() << std::endl;
    }
    grpc::CallbackServerContext *context_;
    const RaftRpc::PreVoteArgs *request_;
    RaftRpc::PreVoteReply* reply_;
    RaftRpcHandler *handler_;
};

grpc::ServerUnaryReactor *RaftServer::PreVote(grpc::CallbackServerContext *context, const RaftRpc::PreVoteArgs *request, RaftRpc::PreVoteReply*reply)
{
    return new PreVoteReactor(context, request, reply, handler_);
}

// Reactor仅用于异步读数据
// 需要耦合Responder、Reactor和handler handler用于执行回调 Responder用于发送响应且Responder用于上层
class InstallSnapshotReactor : public grpc::ServerBidiReactor<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply>
{
public:
    InstallSnapshotReactor(grpc::CallbackServerContext *context, RaftRpcHandler *handler)
        : context_(context),
          handler_(handler),
          closed_(false),
          writing_(false),
          finished_(false)
    {
        auto metadata = context_->client_metadata();
        auto it = metadata.find("node-id");
        if (it != metadata.end())
        {
            nodeId_ = std::string(it->second.data(), it->second.length());
        }
        else
        {
            myAssert(false, "[AppendEntriesReactor] can't get node-id");
        }
        // 流启动
        std::cout << "[start InstallSnapshotStream] peer: " << context_->peer() << std::endl;
        std::unique_ptr<InstallSnapshotResponder> responder = std::make_unique<InstallSnapshotResponderImpl>(this);
        handler_->OnInstallSnapshotStreamOn(std::move(responder), nodeId_); // Raft节点需要记录这个responder 用于后续发送响应
        // Finish(grpc::Status::OK); 没有开始读就关闭流 FIXME:
        StartRead(&pendingRequest_);
    }

    void OnDone() override
    {
        DPrintf("[InstallSnapshotStream completed]");
        delete this;
    }
    void OnCancel() override
    {
        if (!closed_)
        {
            closed_ = true;
            DPrintf("[InstallSnapshotStream Canceled] peer: %s", nodeId_);
            handler_->OnInstallSnapshotStreamClose(nodeId_);
        }
        TryFinish();
    }
    void OnReadDone(bool ok) override
    {
        // 异常关闭
        if (!ok)
        {
            if (!closed_)
            {
                closed_ = true;
                handler_->OnInstallSnapshotStreamClose(nodeId_);
            }
            TryFinish();
            return;
        }
        handler_->OnInstallSnapshotChunk(&pendingRequest_, nodeId_);
        if (!closed_ && !finished_)
            StartRead(&pendingRequest_);
    }
    // 给Responder的接口
    void SendReply(const RaftRpc::InstallSnapshotReply *reply)
    {
        if (finished_ || closed_)
            return;
        pendingReplies_.push(*reply);
        TrySend();
    }
    // 关闭后不能再调用SendReply
    void Close()
    {
        if (finished_)
            return;
        if (!closed_)
        {
            closed_ = true;
            handler_->OnInstallSnapshotStreamClose(nodeId_);
        }
    }

    // FIXME:添加writeDone
    void OnWriteDone(bool ok) override
    {
        writing_ = false;
        if (!ok && !closed_)
        {
            closed_ = true;
            handler_->OnInstallSnapshotStreamClose(nodeId_);
        }
        if (!closed_ && !finished_)
            TrySend();
        else
            TryFinish();
    }
    // 这是给上层的接口
    class InstallSnapshotResponderImpl : public InstallSnapshotResponder
    {
    public:
        InstallSnapshotResponderImpl(InstallSnapshotReactor *reactor)
            : reactor_(reactor),
              closed(false)
        {
        }
        void SendReply(const RaftRpc::InstallSnapshotReply *reply) override
        {
            reactor_->SendReply(reply);
        }
        void Close() override
        {
            reactor_->Close();
            closed = true;
        }

        bool Closed() const { return closed; }

    private:
        InstallSnapshotReactor *reactor_; // FIXME:不能用unique_ptr reactor由grpc管理 生命周期长于responder
        bool closed;
        bool finished_;
        bool writing_;
    };

private:
    void TryFinish()
    {
        if (finished_ || writing_ || !closed_)
            return;
        finished_ = true;
        Finish(grpc::Status::OK);
    }

    void TrySend()
    {
        if (finished_ || closed_ || writing_ || pendingReplies_.empty())
            return;
        writing_ = true;
        pendingReply_ = std::move(pendingReplies_.front());
        pendingReplies_.pop();
        StartWrite(&pendingReply_);
    }

    grpc::CallbackServerContext *context_;
    // RaftRpc::InstallSnapshotArgs *pendingRequest_; 这里不应该是指针 应为对象 FIXME:
    RaftRpc::InstallSnapshotArgs pendingRequest_;
    RaftRpc::InstallSnapshotReply pendingReply_;
    RaftRpcHandler *handler_;

    std::queue<RaftRpc::InstallSnapshotReply> pendingReplies_;
    std::string nodeId_;

    bool closed_;
    bool writing_;
    bool finished_; // 是否调用了Finish
};

grpc::ServerBidiReactor<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply> *RaftServer::InstallSnapshot(grpc::CallbackServerContext *context)
{
    return new InstallSnapshotReactor(context, handler_);
}
// 要注意区分：非正常关闭 写的时候关闭 重复关闭
/*
    执行流为：
    1. 客户端发完数据，调用 WritesDone。服务端 OnReadDone(ok=false) 触发。

    2. 服务端设置 closed_ = true，并调用 TryFinish()。此时如果 writing_ == true（有回复正在发送），TryFinish 直接返回，不关闭。

    3. 上层回调执行 SendReply，将 writing_ 置为 true 并启动 StartWrite。

    4. 稍后 OnWriteDone 被调用，将 writing_ 置为 false，然后调用 TryFinish()。

    5. 此时 closed_ == true 且 writing_ == false，条件满足，执行 Finish() 安全关闭。
*/
class AppendEntriesReactor : public grpc::ServerBidiReactor<RaftRpc::AppendEntriesArgs, RaftRpc::AppendEntriesReply>
{
public:
    AppendEntriesReactor(grpc::CallbackServerContext *context, RaftRpcHandler *handler)
        : context_(context),
          handler_(handler),
          closed_(false),
          writing_(false),
          finished_(false)
    {
        auto metadata = context_->client_metadata();
        auto it = metadata.find("node-id");
        if (it != metadata.end())
        {
            nodeId_ = std::string(it->second.data(), it->second.length());
        }
        else
        {
            myAssert(false, "[AppendEntriesReactor] can't get node-id");
        }
        // 流启动
        std::cout << "[start AppendEntriesStream] peer: " << context_->peer() << std::endl;
        std::unique_ptr<AppendEntriesResponder> responder = std::make_unique<AppendEntriesResponderImpl>(this);
        handler_->OnAppendEntriesStreamOn(std::move(responder), nodeId_);
        // Finish(grpc::Status::OK); //FIXME:
        StartRead(&pendingRequest_);
    }

    void OnDone() override
    {
        DPrintf("[AppendEntriesStream completed]");
        delete this;
    }
    void OnCancel() override
    {
        // 异常关闭
        if (!closed_)
        {
            DPrintf("[AppendEntriesStream Canceled] peer: %s", nodeId_);
            handler_->OnAppendEntriesStreamClose(nodeId_);
        }
        TryFinish();
    }
    // FIXME:
    void OnReadDone(bool ok) override
    {
        if (!ok)
        {
            if (!closed_)
            {
                closed_ = true;
                handler_->OnAppendEntriesStreamClose(nodeId_); // 通知上层
            }
            TryFinish();
            return;
        }
        handler_->OnAppendEntries(&pendingRequest_, nodeId_);
        if (!closed_ && !finished_)
            StartRead(&pendingRequest_); // 不会阻塞
    }
    void SendReply(const RaftRpc::AppendEntriesReply *reply)
    {
        // 避免已经关闭了还在写
        if (finished_ || closed_)
            return;
        pendingReplies_.push(*reply);
        TrySend();
    }
    // 关闭后不能再调用SendReply
    // 关闭职责交给上层
    void Close()
    {
        // 避免重复finish
        if (finished_)
            return;
        if (!closed_)
        {
            closed_ = true;
            handler_->OnAppendEntriesStreamClose(nodeId_);
        }
    }

    void OnWriteDone(bool ok) override
    {
        writing_ = false;
        if (!ok && !closed_)
        {
            closed_ = true;
            handler_->OnAppendEntriesStreamClose(nodeId_);
        }
        if (!finished_ && !closed_)
            TrySend();
        else
            TryFinish(); // 如果对方关闭了还在写 那么这里需要处理对方的关闭
    }
    class AppendEntriesResponderImpl : public AppendEntriesResponder
    {
    public:
        AppendEntriesResponderImpl(AppendEntriesReactor *reactor)
            : reactor_(reactor),
              closed(false)
        {
        }
        void SendReply(const RaftRpc::AppendEntriesReply *reply) override
        {
            reactor_->SendReply(reply);
        }
        void Close() override
        {
            reactor_->Close();
            closed = true;
        }

        bool Closed() const { return closed; }

    private:
        AppendEntriesReactor *reactor_; // FIXME:
        bool closed;
    };

private:
    // 避免重复关闭 并且防止写的时候关闭
    void TryFinish()
    {
        if (finished_ || writing_ || !closed_ || !pendingReplies_.empty())
            return;
        finished_ = true;
        Finish(grpc::Status::OK);
    }
    void TrySend()
    {
        if (finished_ || closed_ || writing_ || pendingReplies_.empty())
            return;
        writing_ = true;
        pendingReply_ = std::move(pendingReplies_.front());
        pendingReplies_.pop();
        StartWrite(&pendingReply_);
    }

    grpc::CallbackServerContext *context_;
    RaftRpc::AppendEntriesArgs pendingRequest_; // FIXME:
    RaftRpc::AppendEntriesReply pendingReply_;
    RaftRpcHandler *handler_;

    std::queue<RaftRpc::AppendEntriesReply> pendingReplies_; // 因为是异步回复，所以存在前一个消息还没发送完毕，后一个消息就开始发了，导致grpc报错TOO_MANY_OPERATIONS
    std::string nodeId_;
    bool closed_;
    bool writing_;
    bool finished_; // 是否已调用Finish
};

grpc::ServerBidiReactor<RaftRpc::AppendEntriesArgs, RaftRpc::AppendEntriesReply> *RaftServer::AppendEntries(grpc::CallbackServerContext *context)
{
    return new AppendEntriesReactor(context, handler_);
}

////////////////////////Client////////////////////////////////
RaftClient::RaftClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(RaftRpc::RaftRpc::NewStub(channel))
{
    DPrintf("[RaftClient Initialized]");
}

// 通过context参数来设置超时
bool RaftClient::RequestVote(grpc::ClientContext *context, RaftRpc::RequestVoteArgs &request, RaftRpc::RequestVoteReply *reply)
{
    auto status = stub_->RequestVote(context, request, reply);
    if (!status.ok())
    {
        DPrintf("[Client RequestVote] something wrong: %s", status.error_message().c_str());
        return false;
    }
    return true;
}

bool RaftClient::PreVote(grpc::ClientContext* context, RaftRpc::PreVoteArgs& request, RaftRpc::PreVoteReply* reply)
{
    auto status = stub_->PreVote(context, request, reply);
    if (!status.ok())
    {
        DPrintf("[Client PreVote] something wrong: %s", status.error_message().c_str());
        return false;
    }
    return true;
}

class InstallSnapshotStreamImpl : public InstallSnapshotStream
{
public:
    InstallSnapshotStreamImpl(std::unique_ptr<grpc::ClientReaderWriter<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply>> rw)
        : rw_(std::move(rw)),
          closed(false)
    {
        DPrintf("[installSnapshotStreamImpl Initialized]");
    }

    bool Write(const RaftRpc::InstallSnapshotArgs *request) override
    {
        return rw_->Write(*request);
    }
    bool Read(RaftRpc::InstallSnapshotReply *reply) override
    {
        return rw_->Read(reply);
    }
    void Close()
    {
        rw_->WritesDone();
        auto status = rw_->Finish();
        closed = true;
        DPrintf("[InstallSnapshotStream Closed] Status: %s", status.error_message().c_str());
    }

    bool Closed() const { return closed; }

private:
    std::unique_ptr<grpc::ClientReaderWriter<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply>> rw_;
    bool closed;
};

std::unique_ptr<InstallSnapshotStream> RaftClient::CreateInstallSnapshotStream(grpc::ClientContext *context)
{
    auto rw = stub_->InstallSnapshot(context);
    auto stream = std::make_unique<InstallSnapshotStreamImpl>(std::move(rw));
    return std::move(stream);
}

class AppendEntriesStreamImpl : public AppendEntriesStream
{
public:
    AppendEntriesStreamImpl(std::unique_ptr<grpc::ClientReaderWriter<RaftRpc::AppendEntriesArgs, RaftRpc::AppendEntriesReply>> rw)
        : rw_(std::move(rw)),
          closed(false)
    {
        DPrintf("[AppendEntriesStreamImpl Initialized]");
    }

    bool Write(const RaftRpc::AppendEntriesArgs *request) override
    {
        return rw_->Write(*request);
    }
    bool Read(RaftRpc::AppendEntriesReply *reply) override
    {
        return rw_->Read(reply);
    }
    void Close()
    {
        // Client方需要先调用WriteDone 通知服务端 再Finish关闭
        rw_->WritesDone();
        auto status = rw_->Finish();
        closed = true;
        DPrintf("[AppendEntriesStream Closed] Status: %s", status.error_message().c_str());
    }

    bool Closed() const { return closed; }

private:
    std::unique_ptr<grpc::ClientReaderWriter<RaftRpc::AppendEntriesArgs, RaftRpc::AppendEntriesReply>> rw_;
    bool closed;
};

std::unique_ptr<AppendEntriesStream> RaftClient::CreateAppendEntriesStream(grpc::ClientContext *context)
{
    auto rw = stub_->AppendEntries(context);
    auto stream = std::make_unique<AppendEntriesStreamImpl>(std::move(rw));
    return std::move(stream);
}