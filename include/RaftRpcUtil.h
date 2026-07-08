#pragma once
#include <memory>
#include <vector>
#include <string>
#include <grpcpp/grpcpp.h>
#include <atomic>

#include "RaftRpc.grpc.pb.h"
#include "RaftRpc.pb.h"
// Server异步接收器
class AppendEntriesResponder
{
public:
    virtual ~AppendEntriesResponder() = default;
    virtual void SendReply(const RaftRpc::AppendEntriesReply *reply) = 0; //发送数据
    virtual void Close() = 0;
    virtual bool Closed() const = 0;
};
class InstallSnapshotResponder
{
public:
    virtual ~InstallSnapshotResponder() = default;
    virtual void SendReply(const RaftRpc::InstallSnapshotReply *reply) = 0; //发送数据
    virtual void Close() = 0;
    virtual bool Closed() const = 0;
};
// Client同步流
class AppendEntriesStream
{
public:
    virtual ~AppendEntriesStream() = default;
    virtual bool Write(const RaftRpc::AppendEntriesArgs *request) = 0;
    virtual bool Read(RaftRpc::AppendEntriesReply *reply) = 0;
    virtual void Close() = 0;
    virtual bool Closed() const = 0;
};
class InstallSnapshotStream
{
public:
    virtual ~InstallSnapshotStream() = default;
    virtual bool Write(const RaftRpc::InstallSnapshotArgs *request) = 0;
    virtual bool Read(RaftRpc::InstallSnapshotReply *reply) = 0;
    virtual void Close() = 0;
    virtual bool Closed() const = 0;
};

// 用于RaftServer回调
// 需要Raft类继承并实现
class RaftRpcHandler
{
public:
    virtual ~RaftRpcHandler() = default;

    virtual void OnRequestVote(const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply) = 0;

    virtual void OnPreVote(const RaftRpc::PreVoteArgs* request, RaftRpc::PreVoteReply* reply) = 0;
    // para peer: the string of node-id, not addr
    virtual void OnAppendEntriesStreamOn(std::unique_ptr<AppendEntriesResponder> responder, const std::string& peer) = 0; //在这里拷贝responder
    virtual void OnAppendEntries(const RaftRpc::AppendEntriesArgs *request, const std::string& peer) = 0; //获取请求信息后 可以用responder发送响应
    virtual void OnAppendEntriesStreamClose(const std::string& peer) = 0;

    virtual void OnInstallSnapshotStreamOn(std::unique_ptr<InstallSnapshotResponder> responder, const std::string& peer) = 0;
    virtual void OnInstallSnapshotChunk(const RaftRpc::InstallSnapshotArgs *request, const std::string& peer) = 0;
    virtual void OnInstallSnapshotStreamClose(const std::string& peer) = 0;
};

// 通信节点 不持有连接
// Server不负责发送 只负责接收request
class RaftServer final : public RaftRpc::RaftRpc::CallbackService
{
public:
    RaftServer(std::string ip, std::string port, RaftRpcHandler *handler);

    void run();      // 启动Raft通信服务器
    void shutdown(); // 关闭服务器

    grpc::ServerUnaryReactor *RequestVote(grpc::CallbackServerContext *context, const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply) override;
    grpc::ServerUnaryReactor* PreVote(grpc::CallbackServerContext* context, const RaftRpc::PreVoteArgs* request, RaftRpc::PreVoteReply* reply) override;
    grpc::ServerBidiReactor<RaftRpc::InstallSnapshotArgs, RaftRpc::InstallSnapshotReply> *InstallSnapshot(grpc::CallbackServerContext *context) override;
    grpc::ServerBidiReactor<RaftRpc::AppendEntriesArgs, RaftRpc::AppendEntriesReply> *AppendEntries(grpc::CallbackServerContext *context) override;

private:
    // 相关回调 由Raft节点传入
    RaftRpcHandler *handler_;

    std::unique_ptr<grpc::Server> server_;
    std::atomic_bool closed_;
};

// 需要持有Channel
// 客户端为同步通信
// 客户端只进行发送数据 接收由上层的Server来处理

class RaftClient
{
public:
    // channel由上层Raft的连接池管理
    RaftClient(std::shared_ptr<grpc::Channel> channel);
    ~RaftClient() = default;
    // RequestVote除外 因为它不涉及流
    bool RequestVote(grpc::ClientContext* context, RaftRpc::RequestVoteArgs &request, RaftRpc::RequestVoteReply *reply);
    bool PreVote(grpc::ClientContext* context, RaftRpc::PreVoteArgs& request, RaftRpc::PreVoteReply* reply);
    //创建流对象 上层根据流来同步通信 超时通过上层的context的deadline来设置（设置的是流的超时时长）
    std::unique_ptr<InstallSnapshotStream> CreateInstallSnapshotStream(grpc::ClientContext *context);
    std::unique_ptr<AppendEntriesStream> CreateAppendEntriesStream(grpc::ClientContext *context);

private:
    std::unique_ptr<RaftRpc::RaftRpc::Stub> stub_;
};