#include <iostream>

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
    DPrintf("[RaftServer Initialized] addr: %s", addr);
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
        std::cout << "[start RequestVoteRpc]" << std::endl;
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
// Reactor仅用于异步读数据
class InstallSnapshotReactor : public grpc::ServerBidiReactor<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply>
{
public:
    InstallSnapshotReactor(grpc::CallbackServerContext *context, RaftRpcHandler *handler)
        : context_(context),
          handler_(handler),
          closed_(false)
    {
        // 流启动
        std::cout << "[start InstallSnapshotStream] peer: " << context_->peer() << std::endl;
        std::unique_ptr<InstallSnapshotResponder> responder = std::make_unique<InstallSnapshotResponderImpl>(this);
        handler_->OnInstallSnapshotStreamOn(std::move(responder)); // Raft节点需要记录这个responder 用于后续发送响应
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
            DPrintf("[InstallSnapshotStream Canceled] peer: %s", context_->peer());
            handler_->OnInstallSnapshotStreamClose();
            Finish(grpc::Status::CANCELLED);
        }
    }
    void OnReadDone(bool ok) override
    {
        if (!ok && !closed_)
        {
            closed_ = true;
            DPrintf("[InstallSnapshotStream Canceled] something wrong while read");
            handler_->OnInstallSnapshotStreamClose();
            Finish(grpc::Status::CANCELLED);
            return;
        }
        handler_->OnInstallSnapshotChunk(&pendingRequest_);
        StartRead(&pendingRequest_);
    }
    void SendReply(const RaftRpc::InstallSnapshotReply *reply)
    {
        pendingReply_ = *reply;
        StartWrite(&pendingReply_);
    }
    //关闭后不能再调用SendReply
    void Close()
    {
        Finish(grpc::Status(grpc::StatusCode::OK, "[InstallSnapshotResponder Closed] Service Closed"));
    }

    // FIXME:添加writeDone
    void OnWriteDone(bool ok) override
    {
        if (!ok && !closed_)
        {
            closed_ = true;
            DPrintf("[InstallSnapshotStream Canceled] something wrong while write");
            handler_->OnInstallSnapshotStreamClose();
            Finish(grpc::Status::CANCELLED);
            return;
        }
        // 写完什么都不做
    }
    // 这是给上层的接口
    class InstallSnapshotResponderImpl : public InstallSnapshotResponder
    {
    public:
        InstallSnapshotResponderImpl(InstallSnapshotReactor *reactor)
            : reactor_(reactor)
        {
        }
        void SendReply(const RaftRpc::InstallSnapshotReply *reply) override
        {
            reactor_->SendReply(reply);
        }
        void Close() override
        {
            reactor_->Close();
        }

    private:
        InstallSnapshotReactor *reactor_; // FIXME:不能用unique_ptr reactor由grpc管理 生命周期长于responder
    };

private:
    grpc::CallbackServerContext *context_;
    // RaftRpc::InstallSnapshotArgs *pendingRequest_; 这里不应该是指针 应为对象 FIXME:
    RaftRpc::InstallSnapshotArgs pendingRequest_;
    RaftRpc::InstallSnapshotReply pendingReply_; // FIXME:需要添加一个缓存响应的变量
    RaftRpcHandler *handler_;
    bool closed_;
};

grpc::ServerBidiReactor<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply> *RaftServer::InstallSnapshot(grpc::CallbackServerContext *context)
{
    return new InstallSnapshotReactor(context, handler_);
}

class AppendEntriesReactor : public grpc::ServerBidiReactor<RaftRpc::AppendEntriesArgs, RaftRpc::AppendEntriesReply>
{
public:
    AppendEntriesReactor(grpc::CallbackServerContext *context, RaftRpcHandler *handler)
        : context_(context),
          handler_(handler),
          closed_(false)
    {
        // 流启动
        std::cout << "[start AppendEntriesStream] peer: " << context_->peer() << std::endl;
        std::unique_ptr<AppendEntriesResponder> responder = std::make_unique<AppendEntriesResponderImpl>(this);
        handler_->OnAppendEntriesStreamOn(std::move(responder));
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
        if (!closed_)
        {
            DPrintf("[AppendEntriesStream Canceled] peer: %s", context_->peer());
            handler_->OnAppendEntriesStreamClose();
            Finish(grpc::Status::CANCELLED);
        }
    }
    void OnReadDone(bool ok) override
    {
        if (!ok && !closed_)
        {
            closed_ = true;
            DPrintf("[AppendEntriesStream Canceled] something wrong while read");
            handler_->OnAppendEntriesStreamClose();
            Finish(grpc::Status::CANCELLED);
            return;
        }
        handler_->OnAppendEntries(&pendingRequest_);
        StartRead(&pendingRequest_);
    }
    void SendReply(const RaftRpc::AppendEntriesReply *reply)
    {
        pendingReply_ = *reply;
        StartWrite(&pendingReply_);
    }
    //关闭后不能再调用SendReply
    void Close()
    {
        Finish(grpc::Status(grpc::StatusCode::OK, "[AppendEntriesReactor Closed] Service Closed"));
    }

    void OnWriteDone(bool ok) override
    {
        if (!ok && !closed_)
        {
            closed_ = true;
            DPrintf("[AppendEntriesStream Canceled] something wrong while write");
            handler_->OnAppendEntriesStreamClose();
            Finish(grpc::Status::CANCELLED);
            return;
        }
    }
    class AppendEntriesResponderImpl : public AppendEntriesResponder
    {
    public:
        AppendEntriesResponderImpl(AppendEntriesReactor *reactor)
            : reactor_(reactor)
        {
        }
        void SendReply(const RaftRpc::AppendEntriesReply *reply) override
        {
            reactor_->SendReply(reply);
        }
        void Close() override
        {
            reactor_->Close();
        }

    private:
        AppendEntriesReactor *reactor_; // FIXME:
    };

private:
    grpc::CallbackServerContext *context_;
    RaftRpc::AppendEntriesArgs pendingRequest_; // FIXME:
    RaftRpc::AppendEntriesReply pendingReply_;
    RaftRpcHandler *handler_;
    bool closed_;
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

//通过context参数来设置超时
bool RaftClient::RequestVote(grpc::ClientContext* context, RaftRpc::RequestVoteArgs &request, RaftRpc::RequestVoteReply *reply)
{
    auto status = stub_->RequestVote(context, request, reply);
    if (!status.ok())
    {
        DPrintf("[Client RequestVote] something wrong");
        return false;
    }
    return true;
}

class InstallSnapshotStreamImpl : public InstallSnapshotStream
{
public:
    InstallSnapshotStreamImpl(std::unique_ptr<grpc::ClientReaderWriter<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply>> rw)
        : rw_(std::move(rw))
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
        DPrintf("[InstallSnapshotStream Closed] Status: %s", status.error_message());
    }

private:
    std::unique_ptr<grpc::ClientReaderWriter<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply>> rw_;
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
        : rw_(std::move(rw))
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
        DPrintf("[AppendEntriesStream Closed] Status: %s", status.error_message());
    }

private:
    std::unique_ptr<grpc::ClientReaderWriter<RaftRpc::AppendEntriesArgs, RaftRpc::AppendEntriesReply>> rw_;
};

std::unique_ptr<AppendEntriesStream> RaftClient::CreateAppendEntriesStream(grpc::ClientContext *context)
{
    auto rw = stub_->AppendEntries(context);
    auto stream = std::make_unique<AppendEntriesStreamImpl>(std::move(rw));
    return std::move(stream);
}