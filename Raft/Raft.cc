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

namespace
{
    // replication state
    constexpr int Disconnected = 0;
    constexpr int Success = 1;
    constexpr int Retry = 2;
    constexpr int BeFollower = 3;
    constexpr int Stopped = 4;
    // vote state
    constexpr int LogOutdated = 0;
    constexpr int Voted = 1;
    constexpr int Expired = 2;
    constexpr int Normal = 3;
    constexpr int NotUpToDate = 4;

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

        lastPeerContact_[entry.first] = Clock::time_point::min();
    }
    myAssert(localAddressFound, "[Raft constructor] local address not in address list");

    readPersist(persister_->readRaftState());
    if (lastIncludeSnapshotIndex_ > 0)
    {
        commitIndex_ = lastIncludeSnapshotIndex_;
        lastApplied_ = lastIncludeSnapshotIndex_;
    }

    const auto now = Clock::now();
    lastResetElectionTime_ = now;
    lastResetHeartBeatTime_ = now;
    resetElectionDeadline();
    resetHeartbeatDeadline();
    resetQuorumDeadline();
    snapshotDeadline_ = now + std::chrono::milliseconds(SnapshotCheckInterval);

    threadPool_ = std::make_unique<ThreadPool>(
        std::max<std::size_t>(2, peers_.size() * 2 + 1));
    // 需要同步关系 这里用promise来同步
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
// 注意析构顺序：先关网络 防止又收到事件 再关线程池 线程池优先级没事件循环高 最后关事件循环
Raft::~Raft()
{
    stop_.store(true);

    if (server_)
    {
        server_->shutdown();
        server_.reset();
    }

    if (threadPool_)
        threadPool_->stop();

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        eventLoopStopping_ = true;
    }
    queueCv_.notify_all();
    if (eventThread_.joinable())
        eventThread_.join();
    // 执行到这里就没有网络事件了
    persist();
    DPrintf("[~Raft] Server %d quit", id_);
}

void Raft::postControl(Event event)
{
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
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

void Raft::eventLoop()
{
    for (;;)
    {
        // 这里需要先处理事件 如果对方的心跳响应和超时同时触发，假如先执行心跳，那么会导致
        // 这个响应无效。但实际上对方早就响应了，因为网络等原因没有及时到达。所以要先处理
        // 网络事件
        processEventBatch();
        handleDeadlines();

        std::unique_lock<std::mutex> lock(queueMutex_);
        if (eventLoopStopping_ && controlQueue_.empty() && commandQueue_.empty())
            break;
        if (!controlQueue_.empty() || !commandQueue_.empty())
            continue;

        // 找最小的ddl 然后阻塞到对应时间点
        auto deadline = snapshotDeadline_;
        if (status_ == RaftRpc::RAFT_LEADER)
        {
            deadline = std::min(deadline, heartbeatDeadline_);
            deadline = std::min(deadline, quorumDeadline_);
        }
        else
            deadline = std::min(deadline, electionDeadline_);
        queueCv_.wait_until(lock, deadline, [this]
                            { return eventLoopStopping_ || !controlQueue_.empty() || !commandQueue_.empty(); });
    }
}

void Raft::processEventBatch()
{
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

    for (std::size_t count = 0; count < CommandBatchSize; ++count)
    {
        // 控制流优先级高于事件流 所以如果执行事件时期超时 那么要重新让给超时
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (!controlQueue_.empty())
                break;
        }
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

void Raft::handleDeadlines()
{
    auto now = Clock::now();
    // 超时事件有四个：checkQuorum heartBeat election和 snapshot
    if (status_ == RaftRpc::RAFT_LEADER)
    {
        // 如果leader在超时周期内没有收到响应 那么要stepdown
        if (now >= quorumDeadline_)
        {
            if (!hasRecentQuorum(now))
            {
                stepDown();
                return;
            }
            resetQuorumDeadline();
        }
        if (now >= heartbeatDeadline_)
        {
            scheduleAllReplication();
            resetHeartbeatDeadline();
        }
    }
    else if (now >= electionDeadline_)
    {
        beginPreVote();
    }

    if (now >= snapshotDeadline_)
    {
        maybeTakeSnapshot();
        snapshotDeadline_ = Clock::now() +
                            std::chrono::milliseconds(SnapshotCheckInterval);
    }
}

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

void Raft::resetQuorumDeadline()
{
    quorumDeadline_ = Clock::now() + std::chrono::milliseconds(ElectionTimeOut);
}

int Raft::clusterSize() const
{
    return static_cast<int>(idToAddr_.size());
}

int Raft::quorumSize() const
{
    return clusterSize() / 2 + 1;
}

void Raft::markPeerActive(int server, Clock::time_point now)
{
    lastPeerContact_[server] = now;
}
// for becomeleader
void Raft::resetLeaderContactTimes(Clock::time_point now)
{
    for (const auto &peer : peers_)
        lastPeerContact_[peer.first] = now;
    resetQuorumDeadline();
}

bool Raft::hasRecentQuorum(Clock::time_point now) const
{
    int active = 1; // leader itself
    const auto lease = std::chrono::milliseconds(ElectionTimeOut);
    for (const auto &peer : peers_)
    {
        auto it = lastPeerContact_.find(peer.first);
        if (it != lastPeerContact_.end() &&
            it->second != Clock::time_point::min() &&
            now - it->second <= lease)
            ++active;
    }
    return active >= quorumSize();
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
            scheduleAllReplication();
        }
        promise->set_value(result); });
    const StartResult result = future.get();
    *newLogIndex = result.index;
    *newLogTerm = result.term;
    *isLeader = result.leader;
}

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
    if (request.term() > currentTerm_)
        becomeFollower(request.term());
    if (request.term() < currentTerm_)
    {
        reply->set_term(currentTerm_);
        reply->set_votegranted(false);
        reply->set_votestate(Expired);
        return;
    }
    /*
        votedFor is null or candidateId, and candidate’s log is at
        least as up-to-date as receiver’s log, grant vote
    */
    const bool mayVote = votedFor_ == -1 || votedFor_ == request.candidateid();
    const bool upToDate = whetherVoteFor(request.lastlogindex(), request.lastlogterm());
    const bool granted = mayVote && upToDate;
    if (granted)
    {
        if (votedFor_ != request.candidateid())
        {
            votedFor_ = request.candidateid();
            persist();
        }
        resetElectionDeadline();
        DPrintf("[handleRequestVote] Server %d vote for %d", id_, request.candidateid());
    }
    reply->set_term(currentTerm_);
    reply->set_votegranted(granted);
    reply->set_votestate(granted ? Normal : (mayVote ? LogOutdated : Voted));
}

void Raft::OnPreVote(const RaftRpc::PreVoteArgs *request,
                     RaftRpc::PreVoteReply *reply)
{
    auto promise = std::make_shared<std::promise<RaftRpc::PreVoteReply>>();
    auto future = promise->get_future();
    RaftRpc::PreVoteArgs copy = *request;
    postControl([this, copy = std::move(copy), promise]
                {
        RaftRpc::PreVoteReply result;
        handlePreVote(copy, &result);
        promise->set_value(std::move(result)); });
    *reply = future.get();
}
// NOTE:PreVote不需要改变节点的状态
void Raft::handlePreVote(const RaftRpc::PreVoteArgs &request,
                         RaftRpc::PreVoteReply *reply)
{
    reply->set_term(currentTerm_);
    if (request.term() < currentTerm_)
    {
        reply->set_granted(false);
        reply->set_votestate(Expired);
        return;
    }
    // 节点同意preVote请求为：whetherVoteFor && 在超时时间内没有leader心跳
    const bool leaseExpired = Clock::now() >= electionDeadline_;
    const bool upToDate =
        whetherVoteFor(request.lastlogindex(), request.lastlogterm());
    const bool granted = leaseExpired && upToDate;
    reply->set_granted(granted);
    reply->set_votestate(granted ? Normal
                                 : (upToDate ? Voted : NotUpToDate));
}

void Raft::OnAppendEntriesStreamOn(std::unique_ptr<AppendEntriesResponder> responder, const std::string &peer)
{
    const int server = std::stoi(peer);
    AppendEntriesResponder *raw = responder.release();
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
        if (status_ != RaftRpc::RAFT_FOLLOWER)
            status_ = RaftRpc::RAFT_FOLLOWER;
        resetElectionDeadline();
        
        bool matches = request.prelogindex() >= lastIncludeSnapshotIndex_ &&
                       request.prelogindex() <= getLastLogIndex() &&
                       matchLog(request.prelogindex(), request.prelogterm());
        if (!matches)
        {
            int conflictIndex = std::min(request.prelogindex(), getLastLogIndex());
            if (conflictIndex <= lastIncludeSnapshotIndex_)
                conflictIndex = lastIncludeSnapshotIndex_ + 1;
            else
            {
                // fast back
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
            for (int i = 0; i < request.entries_size(); ++i)
            {
                const auto &incoming = request.entries(i);
                if (incoming.logindex() <= getLastLogIndex())
                {
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
        status_ = RaftRpc::RAFT_FOLLOWER;
        resetElectionDeadline();
        if (request.lastsnapshotincludeindex() > lastIncludeSnapshotIndex_)
        {
            const int snapshotIndex = request.lastsnapshotincludeindex();
            // 在节点接收到快照时， 可能已经从leader处拿到日志了 所以那部分日志需要保留
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

            ApplyMsg msg;
            msg.SnapshotValid_ = true;
            msg.Snapshot_ = json::parse(request.data());
            msg.SnapshotIndex_ = lastIncludeSnapshotIndex_;
            msg.SnapshotTerm_ = lastIncludeSnapshotTerm_;
            applyQueue_->push(msg);
        }
        reply.set_term(currentTerm_);
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

void Raft::beginPreVote()
{
    if (stop_.load() || status_ == RaftRpc::RAFT_LEADER)
        return;

    status_ = RaftRpc::RAFT_PRECANDIDATE;
    preVoteTerm_ = currentTerm_ + 1;
    preVotesGranted_ = 1;
    const int round = ++preVoteRound_;
    resetElectionDeadline();

    int lastIndex;
    int lastTerm;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    for (const auto &peer : peers_)
    {
        RaftRpc::PreVoteArgs request;
        request.set_term(preVoteTerm_);
        request.set_candidateid(id_);
        request.set_lastlogindex(lastIndex);
        request.set_lastlogterm(lastTerm);
        const int server = peer.first;
        threadPool_->submit([this, server, request, round]() mutable
                            {
            RaftRpc::PreVoteReply reply;
            grpc::ClientContext context;
            context.AddMetadata("node-id", std::to_string(id_));
            context.set_deadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(PreVoteTimeOut));
            const bool ok =
                clients_.at(server)->PreVote(&context, request, &reply);
            postControl([this, server, term = request.term(), round, ok, reply] {
                handlePreVoteReply(server, term, round, ok, reply);
            }); });
    }
    // 为了保证1节点集群也能胜出
    if (preVotesGranted_ >= quorumSize())
        beginElection();
}

void Raft::handlePreVoteReply(int server, int requestTerm, int requestRound,
                              bool transportOk, RaftRpc::PreVoteReply reply)
{
    if (!transportOk)
    {
        // 注意 这里要重置grpc的连接失败的指数退避功能。如果一个节点在分区后恢复，但因为grpc
        // 的指数退避导致节点不能收到心跳，会扰乱集群
        grpc::experimental::ChannelResetConnectionBackoff(
            peers_.at(server).get());
        return;
    }
    if (reply.term() > currentTerm_)
    {
        becomeFollower(reply.term());
        return;
    }
    if (status_ != RaftRpc::RAFT_PRECANDIDATE ||
        requestTerm != preVoteTerm_ || requestRound != preVoteRound_)
        return;
    if (reply.granted() && ++preVotesGranted_ >= quorumSize())
        beginElection();
}

void Raft::beginElection()
{
    if (stop_.load() || status_ == RaftRpc::RAFT_LEADER)
        return;
    status_ = RaftRpc::RAFT_CANDIDATE;
    ++currentTerm_;
    votedFor_ = id_;
    votesGranted_ = 1;
    electionTerm_ = currentTerm_;
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
    if (votesGranted_ >= quorumSize())
        becomeLeader();
}

void Raft::handleVoteReply(int server, int requestTerm, bool transportOk,
                           RaftRpc::RequestVoteReply reply)
{
    if (!transportOk)
    {
        grpc::experimental::ChannelResetConnectionBackoff(
            peers_.at(server).get());
        return;
    }
    if (reply.term() > currentTerm_)
    {
        becomeFollower(reply.term());
        return;
    }
    if (status_ != RaftRpc::RAFT_CANDIDATE ||
        requestTerm != currentTerm_ || electionTerm_ != currentTerm_)
        return;
    if (reply.votegranted() &&
        ++votesGranted_ >= quorumSize())
        becomeLeader();
}
/*
    If election timeout elapses without receiving AppendEntries
    RPC from current leader or granting vote to candidate:
    convert to candidate
*/
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
    preVotesGranted_ = 0;
    resetElectionDeadline();
}

void Raft::stepDown()
{
    // stepDown不需要persist
    status_ = RaftRpc::RAFT_FOLLOWER;
    votesGranted_ = 0;
    preVotesGranted_ = 0;
    // stepDown后就不能持有对应的worker了
    for (auto &entry : replicationPending_)
        entry.second = false;
    for (auto &entry : replicationInFlight_)
        entry.second = false;
    resetElectionDeadline();
}

void Raft::becomeLeader()
{
    status_ = RaftRpc::RAFT_LEADER;
    votesGranted_ = 0;
    preVotesGranted_ = 0;
    const int next = getLastLogIndex() + 1;
    for (const auto &peer : peers_)
    {
        nextIndex_[peer.first] = next;
        matchIndex_[peer.first] = 0;
        replicationInFlight_[peer.first] = false;
        replicationPending_[peer.first] = false;
    }
    resetLeaderContactTimes(Clock::now());
    resetHeartbeatDeadline();
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
    replicationPending_[server] = false;

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
        postControl([this, server, term = request.term(), request, ok, reply] {
            handleAppendReply(server, term, request, ok, reply);
        }); });
}

void Raft::handleAppendReply(int server, int requestTerm,
                             RaftRpc::AppendEntriesArgs request,
                             bool transportOk, RaftRpc::AppendEntriesReply reply)
{
    if (status_ != RaftRpc::RAFT_LEADER || requestTerm != currentTerm_)
        return;
    replicationInFlight_[server] = false;
    if (!transportOk)
    {
        grpc::experimental::ChannelResetConnectionBackoff(
            peers_.at(server).get());
        return;
    }
    if (reply.term() > currentTerm_)
    {
        becomeFollower(reply.term());
        return;
    }
    if (reply.term() < currentTerm_)
        return;
    // 只要收到了消息 就要记录peer的active时间点
    markPeerActive(server, Clock::now());

    bool retry = false;
    if (reply.succss())
    {
        matchIndex_[server] =
            std::max(matchIndex_[server],
                     request.prelogindex() + request.entries_size());
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

    if (retry || replicationPending_[server] ||
        nextIndex_[server] <= getLastLogIndex())
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
    if (status_ != RaftRpc::RAFT_LEADER || requestTerm != currentTerm_)
        return;
    replicationInFlight_[server] = false;
    if (!transportOk)
    {
        grpc::experimental::ChannelResetConnectionBackoff(
            peers_.at(server).get());
        return;
    }
    if (reply.term() > currentTerm_)
    {
        becomeFollower(reply.term());
        return;
    }
    if (reply.term() < currentTerm_)
        return;
    markPeerActive(server, Clock::now());
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
    const int first = previousIndex - lastIncludeSnapshotIndex_;
    for (int i = std::max(0, first); i < static_cast<int>(logs_.size()); ++i)
        *request.add_entries() = logs_[i];
    return request;
}
/*
    If there exists an N such that N > commitIndex, a majority
    of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    set commitIndex = N
*/
void Raft::updateCommitIndex()
{
    const int oldCommit = commitIndex_;
    for (int index = getLastLogIndex(); index > commitIndex_; --index)
    {
        int replicated = 1; // leader itself
        for (const auto &peer : peers_)
            if (matchIndex_[peer.first] >= index)
                ++replicated;
        if (replicated >= quorumSize() &&
            getLogTermFromIndex(index) == currentTerm_)
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

    const int snapshotIndex = commitIndex_;
    const int snapshotTerm = getLogTermFromIndex(snapshotIndex);
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
