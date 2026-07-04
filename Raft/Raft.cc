#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>

#include "../include/Config.h"
#include "../include/Persister.h"
#include "../include/Raft.h"
#include "../include/ThreadPool.h"
#include "../include/json.hpp"

using nlohmann::json;

// like static
namespace
{
    // log replication 状态
    constexpr int Disconnected = 0;
    constexpr int Success = 1;
    constexpr int Retry = 2;
    constexpr int BeFollower = 3;
    constexpr int Stopped = 4;
    constexpr int LogOutdated = 0;
    // vote 状态
    constexpr int Voted = 1;
    constexpr int Expired = 2;
    constexpr int Normal = 3;

    constexpr std::size_t ControlBatchSize = 128;
    constexpr std::size_t CommandBatchSize = 32;
}

Raft::Raft(std::string ip, std::string port,
           std::unordered_map<int, const std::string> idToAddr, int id,
           std::shared_ptr<Persister> persister,
           std::shared_ptr<LockQueue<ApplyMsg>> applyQueue)
    : ip_(std::move(ip)),
      port_(std::move(port)),
      idToAddr_(std::move(idToAddr)),
      persister_(std::move(persister)),
      applyQueue_(std::move(applyQueue)),
      id_(id)
{
    bool localAddressFound = false;
    for (const auto &entry : idToAddr_)
    {
        if (entry.first == id_ && ip_ + ":" + port_ == entry.second)
        {
            localAddressFound = true;
            continue;
        }
        addrToId_.emplace(entry.second, entry.first);
        auto channel = grpc::CreateChannel(entry.second, grpc::InsecureChannelCredentials());
        peers_.emplace(entry.first, channel);
        clients_.emplace(entry.first, std::make_unique<RaftClient>(channel));

        nextIndex_[entry.first] = 1;
        matchIndex_[entry.first] = 0;
        replicationInFlight_[entry.first] = false;
        replicationPending_[entry.first] = false;
    }
    myAssert(localAddressFound, "[Raft constructor] local address not in address list");

    // 需要从宕机前持久化的快照里读内容
    readPersist(persister_->readRaftState());
    if (lastIncludeSnapshotIndex_ > 0)
    {
        commitIndex_ = lastIncludeSnapshotIndex_;
        lastApplied_ = lastIncludeSnapshotIndex_;
    }

    // 重置lastReset时间 开始处理超时事件
    const auto now = Clock::now();
    lastResetElectionTime_ = now;
    lastResetHeartBeatTime_ = now;
    resetElectionDeadline();
    resetHeartbeatDeadline();
    snapshotDeadline_ = now + std::chrono::milliseconds(SnapshotCheckInterval);

    threadPool_ = std::make_unique<ThreadPool>(
        std::max<std::size_t>(2, peers_.size() * 2 + 1));
    // 启动事件循环
    // 用future和promise来同步（用condition_variable也行）
    auto eventReady = std::make_shared<std::promise<void>>();
    auto eventReadyFuture = eventReady->get_future();
    eventThread_ = std::thread([this, eventReady]
                               {
        eventThreadId_ = std::this_thread::get_id();
        eventReady->set_value();
        eventLoop(); });
    eventReadyFuture.get();
    server_ = std::make_unique<RaftServer>(ip_, port_, this);

    DPrintf("[Init/ReInit] Server %d, Term %d, LastIncludeSnapshotIndex %d",
            id_, currentTerm_, lastIncludeSnapshotIndex_);
}
// 关闭的顺序为：网络层、任务线程池，最后到事件队列
Raft::~Raft()
{
    stop_.store(true);

    // Stop new callbacks while the event loop is still able to satisfy callbacks
    // which are already waiting on a promise.
    if (server_)
    {
        server_->shutdown();
        server_.reset();
    }

    // Network jobs may post their final completion events. Drain them before
    // asking the single writer to stop.
    if (threadPool_)
        threadPool_->stop();

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        eventLoopStopping_ = true;
    }
    // 在最后清理事件队列和事件线程
    queueCv_.notify_all();
    if (eventThread_.joinable())
        eventThread_.join();

    // 执行到这里已经结束通信了
    persist();
    DPrintf("[Raft deconstruct] Server %d quit", id_);
}
// 向control队列里添加事件
void Raft::postControl(Event event)
{
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        // 这里要判断队列是否关闭
        if (eventLoopStopping_)
            return;
        controlQueue_.emplace_back(std::move(event));
    }
    queueCv_.notify_one();
}

void Raft::postCommand(Event event)
{
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (eventLoopStopping_)
            return;
        commandQueue_.emplace_back(std::move(event));
    }
    queueCv_.notify_one();
}
// 事件循环
void Raft::eventLoop()
{
    for (;;)
    {
        // 主要工作就是两项：处理超时事件、处理事件队列事件
        // 这里要注意临界情况：假如超时和心跳同时到达，应该先处理心跳。因为网络的抖动更大，而定时的抖动更小 这里应该判定为心跳的优先级更高
        processEventBatch();
        handleDeadlines();
        // 要先处理再退出！
        std::unique_lock<std::mutex> lock(queueMutex_);
        if (eventLoopStopping_ && controlQueue_.empty() && commandQueue_.empty())
            break;
        // 这里判断顺序也要注意：先control 再 command
        if (!controlQueue_.empty() || !commandQueue_.empty())
            continue;

        // 选最近的deadline来等待
        auto deadline = snapshotDeadline_;
        if (status_ == RaftRpc::RAFT_LEADER)
            deadline = std::min(deadline, heartbeatDeadline_);
        else
            deadline = std::min(deadline, electionDeadline_);
        queueCv_.wait_until(lock, deadline, [this]
                            { return eventLoopStopping_ || !controlQueue_.empty() || !commandQueue_.empty(); });
    }
}

void Raft::processEventBatch()
{
    // 先执行control命令
    for (std::size_t count = 0; count < ControlBatchSize; ++count)
    {
        Event event;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (controlQueue_.empty())
                break;
            event = std::move(controlQueue_.front());
            controlQueue_.pop_front();
        }
        event();
    }
    // 再执行command
    for (std::size_t count = 0; count < CommandBatchSize; ++count)
    {
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (!controlQueue_.empty())
                break;
        }
        // 超时判断 如果在执行command时来control命令 要把执行权让出
        if (Clock::now() >= (status_ == RaftRpc::RAFT_LEADER
                                 ? heartbeatDeadline_
                                 : electionDeadline_))
            break;

        Event event;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (commandQueue_.empty())
                break;
            event = std::move(commandQueue_.front());
            commandQueue_.pop_front();
        }
        event();
    }
}
// 处理三个超时任务：选举、心跳、生成快照
void Raft::handleDeadlines()
{
    auto now = Clock::now();
    if (status_ == RaftRpc::RAFT_LEADER)
    {
        if (now >= heartbeatDeadline_)
        {
            DPrintf("[handleDeadlines] Server %d send heartbeat", id_);
            scheduleAllReplication(); // 向所有follower发送心跳
            resetHeartbeatDeadline();
        }
    }
    else if (now >= electionDeadline_)
    {
        DPrintf("[handleDeadlines] Server %d start election", id_);
        beginElection();
    }

    if (now >= snapshotDeadline_)
    {
        maybeTakeSnapshot();
        snapshotDeadline_ = Clock::now() +
                            std::chrono::milliseconds(SnapshotCheckInterval);
    }
}
// 绝对定时
void Raft::resetElectionDeadline()
{
    lastResetElectionTime_ = Clock::now();
    electionDeadline_ = lastResetElectionTime_ + getRandomizedElectionTimeOut();
}

void Raft::resetHeartbeatDeadline()
{
    lastResetHeartBeatTime_ = Clock::now();
    heartbeatDeadline_ = lastResetHeartBeatTime_ +
                         std::chrono::milliseconds(HeartBeatTimeOut);
}

void Raft::start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader)
{
    auto promise = std::make_shared<std::promise<StartResult>>();
    auto future = promise->get_future();
    postCommand([this, command = std::move(command), promise]
                {
        StartResult result;
        if (!stop_.load() && status_ == RaftRpc::RAFT_LEADER)
        {
            RaftRpc::LogEntry entry;
            entry.set_command(command.asString());
            entry.set_logindex(getNewCommandIndex());
            entry.set_logterm(currentTerm_);
            logs_.emplace_back(entry);
            persist();
            result = {entry.logindex(), entry.logterm(), true};
            updateCommitIndex();
            scheduleAllReplication(); // 执行心跳
        }
        promise->set_value(result); });
    const StartResult result = future.get();
    // 更新状态
    *newLogIndex = result.index;
    *newLogTerm = result.term;
    *isLeader = result.leader;
    DPrintf("[start] Server %d add log, command: %s newlogIndex: %d", id_, command.operation.c_str(), *newLogIndex);
}
// control
void Raft::OnRequestVote(const RaftRpc::RequestVoteArgs *request,
                         RaftRpc::RequestVoteReply *reply)
{
    auto promise = std::make_shared<std::promise<RaftRpc::RequestVoteReply>>();
    auto future = promise->get_future();
    RaftRpc::RequestVoteArgs copy = *request;
    postControl([this, copy = std::move(copy), promise]
                {
        RaftRpc::RequestVoteReply result;
        handleRequestVote(copy, &result);
        promise->set_value(std::move(result)); });
    *reply = future.get();
}

void Raft::handleRequestVote(const RaftRpc::RequestVoteArgs &request,
                             RaftRpc::RequestVoteReply *reply)
{
    // 成为follower 但是还是要投票
    if (request.term() > currentTerm_)
        becomeFollower(request.term());
    // term过时了 不需要投票直接拒绝
    if (request.term() < currentTerm_)
    {
        reply->set_term(currentTerm_);
        reply->set_votegranted(false);
        reply->set_votestate(Expired);
        return;
    }
    myAssert(request.term() == currentTerm_, "assert {request.term() == currentTerm_} fail in handleRequestVote");

    // 如果已经投给目标对象了，那么还是要投
    const bool mayVote = votedFor_ == -1 || votedFor_ == request.candidateid();
    const bool upToDate = whetherVoteFor(request.lastlogindex(), request.lastlogterm()); // 根据目标对象的index和term来判断是否投票
    const bool granted = mayVote && upToDate;
    if (granted)
    {
        // 需要更新vote对象
        if (votedFor_ != request.candidateid())
        {
            votedFor_ = request.candidateid();
            persist();
        }
        resetElectionDeadline();
        DPrintf("[handleRequestVote] Server %d received requestVote, vote for %d", id_, votedFor_);
    }
    reply->set_term(currentTerm_);
    reply->set_votegranted(granted);
    reply->set_votestate(granted ? Normal : (mayVote ? LogOutdated : Voted));
}

void Raft::OnAppendEntriesStreamOn(
    std::unique_ptr<AppendEntriesResponder> responder, const std::string &peer)
{
    const int server = std::stoi(peer);
    AppendEntriesResponder *raw = responder.release();
    // 需要保存流对象
    postControl([this, server, raw]
                { streamServer_[server].AEResponder.reset(raw); });
}

void Raft::OnAppendEntries(const RaftRpc::AppendEntriesArgs *request,
                           const std::string &peer)
{
    RaftRpc::AppendEntriesArgs copy = *request;
    const int server = std::stoi(peer);
    postControl([this, copy = std::move(copy), server]
                { handleAppendEntries(copy, server); });
}

void Raft::handleAppendEntries(const RaftRpc::AppendEntriesArgs &request, int server)
{
    RaftRpc::AppendEntriesReply reply;
    reply.set_status(Success);
    if (request.term() > currentTerm_)
        becomeFollower(request.term());
    if (request.term() < currentTerm_)
    {
        reply.set_term(currentTerm_);
        reply.set_succss(false);
        reply.set_updatenextindex(-100);
        reply.set_status(Expired);
    }
    else
    {
        myAssert(request.term() == currentTerm_, "assert {request.term() == currentTerm_} fail in handleAppendEntries");

        if (status_ != RaftRpc::RAFT_FOLLOWER)
            status_ = RaftRpc::RAFT_FOLLOWER;
        resetElectionDeadline();

        bool matches = matchLog(request.prelogindex(), request.prelogterm());
        if (!matches)
        {
            int conflictIndex = std::min(request.prelogindex(), getLastLogIndex());
            if (conflictIndex <= lastIncludeSnapshotIndex_)
                conflictIndex = lastIncludeSnapshotIndex_ + 1;
            else
            {
                // 一直递减到前一个term区域 最多减到lastIncludeSnapshotIndex+1 即index为0的日志
                const int conflictTerm = getLogTermFromIndex(conflictIndex);
                while (conflictIndex > lastIncludeSnapshotIndex_ + 1 &&
                       getLogTermFromIndex(conflictIndex - 1) == conflictTerm)
                    --conflictIndex;
            }
            reply.set_term(currentTerm_);
            reply.set_succss(false);
            reply.set_updatenextindex(conflictIndex);
            reply.set_status(LogOutdated);
        }
        else
        {
            bool changed = false;
            // 一次截断冲突的部分 然后append
            for (int i = 0; i < request.entries_size(); ++i)
            {
                const auto &incoming = request.entries(i);
                if (incoming.logindex() <= getLastLogIndex())
                {
                    // 扩容后默认元素的logindex为0（getlastLogIndex为0） 然后传入的logindex不为0 导致错误
                    // logs_[getSlicesIndexFromLogIndex(request->entries(i).logindex())] = request->entries(i);
                    // 这里避免用getSliceLogIndex
                    const int pos = getSlicesIndexFromLogIndex(incoming.logindex());
                    if (logs_[pos].logterm() != incoming.logterm())
                    {
                        logs_.resize(pos);
                        logs_.push_back(incoming);
                        changed = true;
                    }
                }
                else
                {
                    logs_.push_back(incoming);
                    changed = true;
                }
            }
            if (changed)
                persist();
            if (request.leadercommit() > commitIndex_)
            {
                commitIndex_ = std::min(getLastLogIndex(), request.leadercommit());
                applyCommitted();
            }
            DPrintf("[handleAppendEntries] Server %d got logs, log size: %d, commitindex: %d", id_, request.entries_size(), commitIndex_);
            reply.set_term(currentTerm_);
            reply.set_succss(true);
            reply.set_updatenextindex(-100);
            reply.set_status(Normal);
        }
    }

    auto session = streamServer_.find(server);
    if (session != streamServer_.end() && session->second.AEResponder)
        session->second.AEResponder->SendReply(&reply);
}

void Raft::OnAppendEntriesStreamClose(const std::string &peer)
{
    const int server = std::stoi(peer);
    postControl([this, server]
                {
        auto it = streamServer_.find(server);
        if (it != streamServer_.end() && it->second.AEResponder)
        {
            it->second.AEResponder->Close();
            it->second.AEResponder.reset();
        } });
}

void Raft::OnInstallSnapshotStreamOn(
    std::unique_ptr<InstallSnapshotResponder> responder, const std::string &peer)
{
    const int server = std::stoi(peer);
    InstallSnapshotResponder *raw = responder.release();
    postControl([this, server, raw]
                { streamServer_[server].ISResponder.reset(raw); });
}

void Raft::OnInstallSnapshotChunk(const RaftRpc::InstallSnapshotArgs *request,
                                  const std::string &peer)
{
    RaftRpc::InstallSnapshotArgs copy = *request;
    const int server = std::stoi(peer);
    postControl([this, copy = std::move(copy), server]
                { handleInstallSnapshot(copy, server); });
}

void Raft::handleInstallSnapshot(const RaftRpc::InstallSnapshotArgs &request,
                                 int server)
{
    RaftRpc::InstallSnapshotReply reply;
    if (request.term() > currentTerm_)
        becomeFollower(request.term());
    if (request.term() < currentTerm_)
    {
        reply.set_term(currentTerm_);
    }
    else
    {
        myAssert(request.term() == currentTerm_, "assert {request.term() == currentTerm_} fail in handleInstallSnapshot");

        status_ = RaftRpc::RAFT_FOLLOWER;
        resetElectionDeadline();
        if (request.lastsnapshotincludeindex() > lastIncludeSnapshotIndex_)
        {
            const int snapshotIndex = request.lastsnapshotincludeindex();
            // 是否需要保留日志的后缀部分
            // 因为leader发现follower需要快照时可能已经发过日志了
            const bool keepSuffix =
                snapshotIndex <= getLastLogIndex() &&
                snapshotIndex >= lastIncludeSnapshotIndex_ &&
                getLogTermFromIndex(snapshotIndex) ==
                    request.lastsnapshotincludeterm();
            if (keepSuffix && getLastLogIndex() > snapshotIndex)
            {
                logs_.erase(logs_.begin(),
                            logs_.begin() +
                                getSlicesIndexFromLogIndex(snapshotIndex) + 1);
            }
            else
                logs_.clear();

            lastIncludeSnapshotIndex_ = request.lastsnapshotincludeindex();
            lastIncludeSnapshotTerm_ = request.lastsnapshotincludeterm();
            commitIndex_ = std::max(commitIndex_, lastIncludeSnapshotIndex_);
            lastApplied_ = std::max(lastApplied_, lastIncludeSnapshotIndex_);
            persister_->save(persistData(), request.data());

            // 直接把snapshot传给上层。snapshot是经过leader确认的 可以直接apply
            ApplyMsg msg;
            msg.SnapshotValid_ = true;
            msg.Snapshot_ = json::parse(request.data());
            msg.SnapshotIndex_ = lastIncludeSnapshotIndex_;
            msg.SnapshotTerm_ = lastIncludeSnapshotTerm_;
            applyQueue_->push(msg);
        }
        reply.set_term(currentTerm_);
        DPrintf("[handleInstallSnapshot] Server %d got Snapshot from %d", id_, request.leaderid());
    }
    auto session = streamServer_.find(server);
    if (session != streamServer_.end() && session->second.ISResponder)
        session->second.ISResponder->SendReply(&reply);
}

void Raft::OnInstallSnapshotStreamClose(const std::string &peer)
{
    const int server = std::stoi(peer);
    postControl([this, server]
                {
        auto it = streamServer_.find(server);
        if (it != streamServer_.end() && it->second.ISResponder)
        {
            it->second.ISResponder->Close();
            it->second.ISResponder.reset();
        } });
}

void Raft::beginElection()
{
    if (stop_.load() || status_ == RaftRpc::RAFT_LEADER)
        return;
    status_ = RaftRpc::RAFT_CANDIDATE;
    ++currentTerm_;
    votedFor_ = id_;
    votesGranted_ = 1;            // counter for vote
    electionTerm_ = currentTerm_; // save term before election
    persist();
    resetElectionDeadline();

    int lastIndex;
    int lastTerm;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    for (const auto &peer : peers_)
    {
        RaftRpc::RequestVoteArgs request;
        request.set_term(currentTerm_);
        request.set_candidateid(id_);
        request.set_lastlogindex(lastIndex);
        request.set_lastlogterm(lastTerm);
        const int server = peer.first;
        threadPool_->submit([this, server, request]() mutable
                            {
            RaftRpc::RequestVoteReply reply;
            grpc::ClientContext context;
            context.AddMetadata("node-id", std::to_string(id_));
            context.set_deadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(ElectionTimeOut));
            const bool ok =
                clients_.at(server)->RequestVote(&context, request, &reply);
            postControl([this, server, term = request.term(), ok, reply] {
                handleVoteReply(server, term, ok, reply);
            }); });
    }
    // 如果集群大小为1那么也要成为leader
    if (votesGranted_ >= static_cast<int>(idToAddr_.size() / 2 + 1))
        becomeLeader();
}

void Raft::handleVoteReply(int, int requestTerm, bool transportOk,
                           RaftRpc::RequestVoteReply reply)
{
    if (!transportOk)
        return;
    if (reply.term() > currentTerm_)
    {
        becomeFollower(reply.term());
        return;
    }
    // 执行becomeleader后 后续handleVoteReply就不会重复执行了
    if (status_ != RaftRpc::RAFT_CANDIDATE ||
        requestTerm != currentTerm_ || electionTerm_ != currentTerm_)
        return;
    if (reply.votegranted() &&
        ++votesGranted_ >= static_cast<int>(idToAddr_.size() / 2 + 1))
        becomeLeader();
}
// 更新状态、持久化，并重置electionTimeout
void Raft::becomeFollower(int term)
{
    if (term > currentTerm_)
    {
        currentTerm_ = term;
        votedFor_ = -1;
        persist();
    }
    status_ = RaftRpc::RAFT_FOLLOWER;
    votesGranted_ = 0;
    resetElectionDeadline();
    DPrintf("[becomeFollower] Server %d be follower", id_);
}

void Raft::becomeLeader()
{
    status_ = RaftRpc::RAFT_LEADER;
    const int next = getLastLogIndex() + 1;
    for (const auto &peer : peers_)
    {
        nextIndex_[peer.first] = next;
        matchIndex_[peer.first] = 0;
        replicationPending_[peer.first] = false;
    }
    DPrintf("[becomeleader] Server %d be leader", id_);
    // 重置心跳超时并作为leader立即发送心跳
    resetHeartbeatDeadline();
    DPrintf("[becomeleader] Server %d heartbeat for all follower", id_);

    RaftRpc::LogEntry entry;
    entry.set_command("no-op");
    entry.set_logindex(getNewCommandIndex());
    entry.set_logterm(currentTerm_);
    logs_.emplace_back(entry);

    scheduleAllReplication();
}

void Raft::scheduleAllReplication()
{
    if (status_ != RaftRpc::RAFT_LEADER || stop_.load())
        return;
    for (const auto &peer : peers_)
        scheduleReplication(peer.first);
}

void Raft::scheduleReplication(int server)
{
    if (stop_.load() || status_ != RaftRpc::RAFT_LEADER)
        return;
    if (replicationInFlight_[server])
    {
        replicationPending_[server] = true;
        return;
    }
    replicationInFlight_[server] = true;
    replicationPending_[server] = false; // 消费pending

    if (nextIndex_[server] <= lastIncludeSnapshotIndex_)
    {
        RaftRpc::InstallSnapshotArgs request;
        request.set_leaderid(id_);
        request.set_term(currentTerm_);
        request.set_lastsnapshotincludeindex(lastIncludeSnapshotIndex_);
        request.set_lastsnapshotincludeterm(lastIncludeSnapshotTerm_);
        request.set_data(persister_->readSnapShot());
        request.set_offset(0);
        launchSnapshot(server, std::move(request));
    }
    else
        launchAppendEntries(server, buildAppendEntries(server));
}

void Raft::launchAppendEntries(int server, RaftRpc::AppendEntriesArgs request)
{
    threadPool_->submit([this, server, request = std::move(request)]
                        {
        RaftRpc::AppendEntriesReply reply;
        grpc::ClientContext context;
        context.AddMetadata("node-id", std::to_string(id_));
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(AppendEntriesTimeOut));
        auto stream = clients_.at(server)->CreateAppendEntriesStream(&context);

        const bool ok = stream->Write(&request) && stream->Read(&reply);
        stream->Close();

        // control
        postControl([this, server, term = request.term(), request, ok, reply] {
            handleAppendReply(server, term, request, ok, reply);
        }); });
}
// 处理append结果
void Raft::handleAppendReply(int server, int requestTerm,
                             RaftRpc::AppendEntriesArgs request,
                             bool transportOk, RaftRpc::AppendEntriesReply reply)
{
    replicationInFlight_[server] = false;
    if (status_ != RaftRpc::RAFT_LEADER || requestTerm != currentTerm_)
        return;
    if (!transportOk)
    {
        // NOTE:如果一个节点从分区中恢复，那么可能因为grpc的指数退避而超时触发选举
        grpc::experimental::ChannelResetConnectionBackoff(peers_.at(server).get());
    }
    // 凡是收到消息 就要判断term
    if (reply.term() > currentTerm_)
    {
        becomeFollower(reply.term());
        return;
    }
    if (reply.term() < currentTerm_)
        return;

    bool retry = false;
    if (reply.succss())
    {
        // 幂等
        matchIndex_[server] = std::max(matchIndex_[server],request.prelogindex() + request.entries_size());
        nextIndex_[server] = matchIndex_[server] + 1;
        updateCommitIndex();
    }
    else
    {
        int suggested = reply.updatenextindex();
        if (suggested == -100)
            suggested = nextIndex_[server] - 1;

        suggested = std::max(1, std::min(suggested, getLastLogIndex() + 1));
        if (suggested >= nextIndex_[server])
            suggested = std::max(1, nextIndex_[server] - 1);
        nextIndex_[server] = suggested;
        retry = true;
    }
    // 如果日志没有全部复制过去 那么还是要启动复制的
    if (retry || replicationPending_[server] || nextIndex_[server] <= getLastLogIndex())
        scheduleReplication(server);
}

void Raft::launchSnapshot(int server, RaftRpc::InstallSnapshotArgs request)
{
    threadPool_->submit([this, server, request = std::move(request)]
                        {
        RaftRpc::InstallSnapshotReply reply;
        grpc::ClientContext context;
        context.AddMetadata("node-id", std::to_string(id_));
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(InstallSnapshotTimeOut));
        auto stream = clients_.at(server)->CreateInstallSnapshotStream(&context);
        const bool ok = stream->Write(&request) && stream->Read(&reply);
        stream->Close();
        postControl([this, server, term = request.term(), ok, reply] {
            handleSnapshotReply(server, term, ok, reply);
        }); });
}

void Raft::handleSnapshotReply(int server, int requestTerm, bool transportOk,
                               RaftRpc::InstallSnapshotReply reply)
{
    replicationInFlight_[server] = false;
    if (status_ != RaftRpc::RAFT_LEADER || requestTerm != currentTerm_)
        return;
    if (!transportOk)
    {
        grpc::experimental::ChannelResetConnectionBackoff(peers_.at(server).get());
        return;
    }
    if (reply.term() > currentTerm_)
    {
        becomeFollower(reply.term());
        return;
    }
    matchIndex_[server] = std::max(matchIndex_[server], lastIncludeSnapshotIndex_);
    nextIndex_[server] = matchIndex_[server] + 1;
    if (replicationPending_[server] || nextIndex_[server] <= getLastLogIndex())
        scheduleReplication(server);
}

RaftRpc::AppendEntriesArgs Raft::buildAppendEntries(int server)
{
    RaftRpc::AppendEntriesArgs request;
    int previousIndex;
    int previousTerm;
    getPrevLogInfo(server, &previousIndex, &previousTerm);
    request.set_term(currentTerm_);
    request.set_leaderid(id_);
    request.set_prelogindex(previousIndex);
    request.set_prelogterm(previousTerm);
    request.set_leadercommit(commitIndex_);
    // 注意边界条件
    const int first = previousIndex - lastIncludeSnapshotIndex_;
    for (int i = std::max(0, first); i < static_cast<int>(logs_.size()); ++i)
        *request.add_entries() = logs_[i];
    return request;
}

void Raft::updateCommitIndex()
{
    const int oldCommit = commitIndex_;
    for (int index = getLastLogIndex(); index > commitIndex_; --index)
    {
        int replicated = 1; // leader itself
        for (const auto &peer : peers_)
            if (matchIndex_[peer.first] >= index)
                ++replicated;
        // 根据Raft安全性要求， 提交日志的话日志里必须有一条当前term的日志
        // 比较最新的一条日志即可
        if (replicated >= static_cast<int>(idToAddr_.size() / 2 + 1) && getLogTermFromIndex(index) == currentTerm_)
        {
            commitIndex_ = index;
            break;
        }
    }
    if (commitIndex_ != oldCommit)
        applyCommitted();
}

void Raft::applyCommitted()
{
    while (lastApplied_ < commitIndex_)
    {
        ++lastApplied_;
        ApplyMsg msg;
        msg.CommandValid_ = true;
        msg.Command_ = logs_[getSlicesIndexFromLogIndex(lastApplied_)].command();
        msg.CommandIndex_ = lastApplied_;
        msg.SnapshotValid_ = false;
        applyQueue_->push(msg);
    }
}

void Raft::maybeTakeSnapshot()
{
    if (status_ != RaftRpc::RAFT_LEADER ||
        commitIndex_ - lastIncludeSnapshotIndex_ < SnapshotThreshold ||
        !genSnapshotCallback_)
        return;

    DPrintf("[takeSnapshot] Server %d generating snapshot", id_);
    const int snapshotIndex = commitIndex_;
    const int snapshotTerm = getLogTermFromIndex(snapshotIndex);

    // 上层给的snapshotgenerater
    std::string data = genSnapshotCallback_();

    const int eraseCount = snapshotIndex - lastIncludeSnapshotIndex_;
    logs_.erase(logs_.begin(), logs_.begin() + eraseCount);
    lastIncludeSnapshotIndex_ = snapshotIndex;
    lastIncludeSnapshotTerm_ = snapshotTerm;
    lastApplied_ = std::max(lastApplied_, snapshotIndex);
    persister_->save(persistData(), data);
}

bool Raft::matchLog(int logIndex, int logTerm)
{
    if (logIndex < lastIncludeSnapshotIndex_ || logIndex > getLastLogIndex())
        return false;
    return getLogTermFromIndex(logIndex) == logTerm;
}

void Raft::persist()
{
    persister_->saveRaftState(persistData());
}

bool Raft::whetherVoteFor(int logIndex, int term)
{
    int lastIndex;
    int lastTerm;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    return term > lastTerm || (term == lastTerm && logIndex >= lastIndex);
}

int Raft::getNewCommandIndex()
{
    return getLastLogIndex() + 1;
}

void Raft::getPrevLogInfo(int server, int *preIndex, int *preTerm)
{
    *preIndex = nextIndex_[server] - 1;
    *preTerm = getLogTermFromIndex(*preIndex);
}

int Raft::getLastLogIndex()
{
    return logs_.empty() ? lastIncludeSnapshotIndex_ : logs_.back().logindex();
}

int Raft::getLastLogTerm()
{
    return logs_.empty() ? lastIncludeSnapshotTerm_ : logs_.back().logterm();
}

void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm)
{
    *lastLogIndex = getLastLogIndex();
    *lastLogTerm = getLastLogTerm();
}

int Raft::getLogTermFromIndex(int logIndex)
{
    myAssert(logIndex >= lastIncludeSnapshotIndex_ &&
                 logIndex <= getLastLogIndex(),
             "[getLogTermFromIndex] invalid log index");
    if (logIndex == lastIncludeSnapshotIndex_)
        return lastIncludeSnapshotTerm_;
    return logs_[getSlicesIndexFromLogIndex(logIndex)].logterm();
}

int Raft::getSlicesIndexFromLogIndex(int logIndex)
{
    const int result = logIndex - lastIncludeSnapshotIndex_ - 1;
    myAssert(result >= 0 && result < static_cast<int>(logs_.size()),
             "[getSlicesIndexFromLogIndex] invalid log index");
    return result;
}

std::vector<ApplyMsg> Raft::getApplyLogs()
{
    std::vector<ApplyMsg> result;
    while (lastApplied_ < commitIndex_)
    {
        ++lastApplied_;
        ApplyMsg msg;
        msg.CommandValid_ = true;
        msg.Command_ = logs_[getSlicesIndexFromLogIndex(lastApplied_)].command();
        msg.CommandIndex_ = lastApplied_;
        msg.SnapshotValid_ = false;
        result.push_back(std::move(msg));
    }
    return result;
}

void Raft::pushMsgToKvServer(ApplyMsg msg)
{
    applyQueue_->push(std::move(msg));
}

int Raft::getRaftStateSize()
{
    return persister_->raftStateSize();
}

void Raft::readPersist(std::string value)
{
    if (value.empty())
        return;
    json state = json::parse(value);
    currentTerm_ = state.value("currentTerm", 0);
    votedFor_ = state.value("votedFor", -1);
    lastIncludeSnapshotIndex_ = state.value("lastIncludeSnapshotIndex", 0);
    lastIncludeSnapshotTerm_ = state.value("lastIncludeSnapshotTerm", 0);
    if (state.contains("logs") && state["logs"].is_array())
    {
        for (const auto &item : state["logs"])
        {
            RaftRpc::LogEntry entry;
            entry.set_logterm(item.value("logTerm", 0));
            entry.set_logindex(item.value("logIndex", 0));
            entry.set_command(item.value("command", std::string{}));
            logs_.push_back(std::move(entry));
        }
    }
}

std::string Raft::persistData()
{
    json state = {
        {"currentTerm", currentTerm_},
        {"votedFor", votedFor_},
        {"lastIncludeSnapshotIndex", lastIncludeSnapshotIndex_},
        {"lastIncludeSnapshotTerm", lastIncludeSnapshotTerm_}};
    state["logs"] = json::array();
    for (const auto &entry : logs_)
        state["logs"].push_back({{"logTerm", entry.logterm()},
                                 {"logIndex", entry.logindex()},
                                 {"command", entry.command()}});
    return state.dump(4);
}

const std::string Raft::getAddrById(int id)
{
    auto it = idToAddr_.find(id);
    myAssert(it != idToAddr_.end(), "[getAddrById] unknown id");
    return it->second;
}

void Raft::getState(int *term, bool *isLeader)
{
    auto state = query<std::pair<int, bool>>([this]
                                             { return std::make_pair(currentTerm_, status_ == RaftRpc::RAFT_LEADER); });
    *term = state.first;
    *isLeader = state.second;
}

int Raft::getCurrentTerm()
{
    return query<int>([this]
                      { return currentTerm_; });
}

int Raft::getVotedFor()
{
    return query<int>([this]
                      { return votedFor_; });
}

std::vector<RaftRpc::LogEntry> Raft::getLogs()
{
    return query<std::vector<RaftRpc::LogEntry>>([this]
                                                 { return logs_; });
}

int Raft::getCommitIndex()
{
    return query<int>([this]
                      { return commitIndex_; });
}

int Raft::getLastApplied()
{
    return query<int>([this]
                      { return lastApplied_; });
}

std::unordered_map<int, int> Raft::getNextIndex()
{
    return query<std::unordered_map<int, int>>([this]
                                               { return nextIndex_; });
}

std::unordered_map<int, int> Raft::getMatchIndex()
{
    return query<std::unordered_map<int, int>>([this]
                                               { return matchIndex_; });
}

RaftRpc::RaftState Raft::getStatus()
{
    return query<RaftRpc::RaftState>([this]
                                     { return status_; });
}

std::chrono::steady_clock::time_point Raft::getLastResetElectionTime()
{
    return query<Clock::time_point>([this]
                                    { return lastResetElectionTime_; });
}

std::chrono::steady_clock::time_point Raft::LastResetHeartBeatTime()
{
    return query<Clock::time_point>([this]
                                    { return lastResetHeartBeatTime_; });
}

int Raft::getLastIncludeSnapshotIndex()
{
    return query<int>([this]
                      { return lastIncludeSnapshotIndex_; });
}

int Raft::getLastIncludeSnapshotTerm()
{
    return query<int>([this]
                      { return lastIncludeSnapshotTerm_; });
}
// for upon
void Raft::setSnapshotCallback(const std::function<std::string()> &generator)
{
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    postControl([this, generator, promise]
                {
        genSnapshotCallback_ = generator;
        promise->set_value(); });
    future.get();
}
