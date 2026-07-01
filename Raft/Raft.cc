#include <cmath>
#include <thread>

#include "../include/Raft.h"
#include "../include/Persister.h"
#include "../include/Config.h"
#include "../include/json.hpp"
#include "../include/ThreadPool.h"

using nlohmann::json;

// 心跳状态
static constexpr int Disconnected = 0;
static constexpr int Success = 1;
static constexpr int Retry = 2;
static constexpr int BeFollower = 3;
static constexpr int Stopped = 4;

// Vote状态
static constexpr int LogOutdated = 0; // 日志不够新
static constexpr int Voted = 1;       // 已经投过票了 拒绝
static constexpr int Expired = 2;     // TERM太旧了
static constexpr int Normal = 3;      // 正常投票

// peers为所有节点的id
Raft::Raft(std::string ip, std::string port, std::unordered_map<int, const std::string> idToAddr, int id, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyQueue)
    : ip_(ip),
      port_(port),
      idToAddr_(idToAddr),
      persister_(persister),
      id_(id),
      currentTerm_(0),
      votedFor_(-1),
      commitIndex_(0),
      lastApplied_(0),
      status_(RaftRpc::RaftState::RAFT_FOLLOWER),
      stop_(false),
      applyQueue_(applyQueue),
      lastResetElectionTime_(std::chrono::steady_clock::now()),
      lastResetHeartBeatTime_(std::chrono::steady_clock::now()),
      lastIncludeSnapshotIndex_(0),
      lastIncludeSnapshotTerm_(0)
{
    // std::lock_guard<std::mutex> lock(mutex_); Raft节点初始化好像没有必要加锁

    server_ = std::make_unique<RaftServer>(ip_, port_, this);
    threadPool_ = std::make_unique<ThreadPool>(std::max((size_t)2, peers_.size() + 1));

    bool inList = false;
    for (auto i = idToAddr_.begin(); i != idToAddr_.end(); i++)
    {
        if (i->first == id_ && ip_ + ":" + port_ == i->second)
        {
            inList = true;
            continue;
        }
        addrToId_.emplace(i->second, i->first);

        auto channel = grpc::CreateChannel(i->second, grpc::InsecureChannelCredentials());
        peers_.emplace(i->first, channel);

        auto client = std::make_unique<RaftClient>(channel);
        clients_.emplace(i->first, std::move(client));
        /*
            使用局部流
        */
        // context的生命周期要比channel长
        // FollowerSession session;
        // session.context = std::make_unique<grpc::ClientContext>();
        // session.context->set_deadline();
        // session.AEStream = clients_[i->first]->CreateAppendEntriesStream(session.context.get());
        // session.ISStream = clients_[i->first]->CreateInstallSnapshotStream(session.context.get());
        // streamClient_.emplace(session);

        matchIndex_[i->first] = 0;
        nextIndex_[i->first] = 0;
    }
    myAssert(inList, "[Raft constructer] localAddr not in AddrList!");

    // 崩溃后需要读已持久化的状态
    // 会把持久化的内容读到对象里 包括：currentTerm, votedFor, logs,lastIncludeSnapshotIndex lastIncludeSnapshotTerm
    readPersist(persistData());
    // 如果读出来有快照 则把快照读出来
    if (lastIncludeSnapshotIndex_ > 0)
    {
        lastApplied_ = lastIncludeSnapshotIndex_;
    }
    DPrintf("[Init/ReInit] Server %d, Term %d, LastIncludeSnapshotIndex %d, LastIncludeSnapshotTerm %d", id_, currentTerm_, lastIncludeSnapshotIndex_, lastIncludeSnapshotTerm_);

    // 启动三个线程
    // 心跳线程 用于保持连接
    leaderHeartBeatTickerThread_ = std::make_unique<std::thread>([this]()
                                                                 { this->leaderHeartBeatTicker(); });
    // 选举线程 用于超时后发起选举
    electionTimeOutTickerThread_ = std::make_unique<std::thread>([this]()
                                                                 { this->electionTimeOutTicker(); });
    // 应用线程 用于定时应用日志
    applierTickerThread_ = std::make_unique<std::thread>([this]()
                                                         { this->applierTicker(); });
}

Raft::~Raft()
{
    stop_ = true;

    if (leaderHeartBeatTickerThread_ && leaderHeartBeatTickerThread_->joinable())
        leaderHeartBeatTickerThread_->join();
    if (electionTimeOutTickerThread_ && electionTimeOutTickerThread_->joinable())
        electionTimeOutTickerThread_->join();
    if (applierTickerThread_ && applierTickerThread_->joinable())
        applierTickerThread_->join();

    if (threadPool_)
        threadPool_->stop();

    myAssert(threadPool_->isStopped(), "[~Raft] threadPool deconstruct error");
    // 关闭 server 等资源
    if (server_)
        server_->shutdown();

    persist();
}
// 提供给上层的接口
//  根据上层的命令，来保存到日志，并传出该日志项的index和term
void Raft::start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (status_ != RaftRpc::RaftState::RAFT_LEADER)
    {
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }

    RaftRpc::LogEntry newlogEntry;
    // RaftRpc.proto里的command定义的是bytes类型 而command是字符串 所以可以
    newlogEntry.set_command(command.asString());
    newlogEntry.set_logindex(getNewCommandIndex());
    newlogEntry.set_logterm(currentTerm_);
    // 添加到日志里
    logs_.emplace_back(newlogEntry);
    // 等到下一次心跳后，再向follower传递新的日志 即不会立即传递数据
    // TODO:应该立即发送AE
    persist();
    *newLogIndex = newlogEntry.logindex();
    *newLogTerm = newlogEntry.logterm();
    *isLeader = true;
}
// TODO: 被动接收
void Raft::OnRequestVote(const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply)
{
    std::lock_guard<std::mutex> lock(mutex_);
    // 凡是收到消息 都要判断term
    if (request->term() > currentTerm_)
    {
        currentTerm_ = request->term();
        votedFor_ = -1;
        status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
        persist();
        lastResetElectionTime_ = std::chrono::steady_clock::now();
        DPrintf("[OnRequestVote] Server %d be follower", id_);
        // return; 不能return 还要判断是否投票
    }
    else if (request->term() < currentTerm_)
    {
        reply->set_term(currentTerm_);
        reply->set_votegranted(false);
        reply->set_votestate(Expired); // term太旧了
        DPrintf("[OnRequestVote] Server %d, request from %d refused cause term outdated", id_, request->candidateid());
    }

    myAssert(request->term() == currentTerm_, "assert {request->term() == currentTerm_} fail");
    // 投过票了
    if (votedFor_ != -1)
    {
        // 拒绝
        if (votedFor_ != request->candidateid())
        {
            reply->set_term(currentTerm_);
            reply->set_votegranted(false);
            reply->set_votestate(Voted);
            DPrintf("[OnRequestVote] node %d refuse vote to %d, cause had voted", id_, request->candidateid());
        }
        // 需要幂等处理
        else
        {
            reply->set_term(currentTerm_);
            reply->set_votegranted(true);
            reply->set_votestate(Normal);
        }
    }
    // 没有投过票
    else
    {
        bool vote = whetherVoteFor(request->lastlogindex(), request->lastlogterm());
        if (vote)
        {
            reply->set_term(currentTerm_);
            reply->set_votegranted(true);
            reply->set_votestate(Normal);

            votedFor_ = request->candidateid();
            persist();
            DPrintf("[OnRequestVote] Server %d, vote for %d", id_, request->candidateid());
        }
        else
        {
            reply->set_term(currentTerm_);
            reply->set_votegranted(false);
            reply->set_votestate(LogOutdated);
            DPrintf("[OnRequestVote] Server %d, Refuse vote for %d: currentTerm %d, request term %d", id_, request->candidateid(), currentTerm_, request->term());
        }
    }
}
void Raft::OnAppendEntriesStreamOn(std::unique_ptr<AppendEntriesResponder> responder, const std::string &nodeId)
{
    // 不能这样找id，从client端来的请求地址是没有指定的、随机分配的，所以根本不可能找得到
    // int id = getIdByAddr(peer);
    int id = std::stoi(nodeId);

    DPrintf("[OnAppendEntriesStreamOn] responder got, from %d", id);
    // 保存对端Channel
    streamServer_[id].AEResponder = std::move(responder);
    myAssert(streamServer_[id].AEResponder != nullptr, "[OnAppendEntriesStreamOn] responder save fail");
}
void Raft::OnAppendEntries(const RaftRpc::AppendEntriesArgs *request, const std::string &nodeId)
{
    int id = std::stoi(nodeId);

    RaftRpc::AppendEntriesReply reply;
    reply.set_status(Success); // success为网络状态
    std::lock_guard<std::mutex> lock(mutex_);
    // 收到消息 判断term
    if (request->term() > currentTerm_)
    {
        currentTerm_ = request->term();
        votedFor_ = -1;
        status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
        persist();
        lastResetElectionTime_ = std::chrono::steady_clock::now();
    }
    else if (request->term() < currentTerm_)
    {
        reply.set_term(currentTerm_);
        reply.set_succss(false);         // 心跳失败
        reply.set_updatenextindex(-100); // 这里什么值都无所谓 让领导者更新自己的term
        reply.set_status(Expired);
        // 从过期的领导者获取的心跳不需要重置定时器
        DPrintf("[OnAppendEntries] follower refused cause leader's term outdated");
        streamServer_[id].AEResponder->SendReply(&reply);
        return;
    }

    myAssert(request->term() == currentTerm_, "assert {request->term() == currentTerm_} fail");
    // 要重置electionTicker的计时
    lastResetElectionTime_ = std::chrono::steady_clock::now();

    // 正常心跳 没有带日志
    if (request->entries_size() == 0)
    {
        DPrintf("[OnAppendEntries] normal heartbeat from %d", id);
        if (request->leadercommit() > commitIndex_)
            commitIndex_ = std::min(getLastLogIndex(), request->leadercommit());
        myAssert(commitIndex_ <= getLastLogIndex(), "[OnAppendEntries] heartbeat commitIndex error");
        reply.set_term(currentTerm_);
        reply.set_succss(true);
        reply.set_updatenextindex(-100);
        reply.set_status(Normal);
        streamServer_[id].AEResponder->SendReply(&reply);
        return;
    }

    // 本节点与请求的prelogindex和prelogterm来对比 根据对比结果来判断是否接收日志
    if (matchLog(request->prelogindex(), request->prelogterm()))
    {
        DPrintf("[OnAppendEntries] Server %d log accept", id_);
        if (request->entries(0).logindex() > getLastLogIndex())
        {
            logs_.insert(logs_.end(), request->entries().begin(), request->entries().begin() + request->entries_size());
        }
        else
        {
            // Raft标准是先截断后续冲突部分 再复制
            // 为了提高效率和幂等，这里先覆盖再截断
            auto pos = getSlicesIndexFromLogIndex(request->entries(0).logindex());
            if (pos + request->entries_size() > logs_.size())
                logs_.resize(logs_.size() + request->entries_size());

            for (int i = 0; i < request->entries_size(); i++)
            {
                logs_[getSlicesIndexFromLogIndex(request->entries(i).logindex())] = request->entries(i);
                myAssert(logs_[getSlicesIndexFromLogIndex(request->entries(i).logindex())].logterm() == request->entries(i).logterm(), "[OnAppendEntries] copy logEntry error, term error");
                if (i == request->entries_size() - 1)
                {
                    logs_.resize(getSlicesIndexFromLogIndex(request->entries(i).logindex()) + 1);
                }
            }
        }
        myAssert(getLastLogIndex() == request->prelogindex() + request->entries_size(), "[OnAppendEntries] log replicate fail");

        // 更新commitIndex
        if (request->leadercommit() > commitIndex_)
            commitIndex_ = std::min(getLastLogIndex(), request->leadercommit());
        
        myAssert(getLastLogIndex() >= commitIndex_, "[OnAppendEntries] commitIndex greater than lastLogIndex");
        reply.set_term(currentTerm_);
        reply.set_succss(true);
        reply.set_updatenextindex(-100); // 没有用这个的必要
        reply.set_status(Normal);

        streamServer_[id].AEResponder->SendReply(&reply);
    }
    // 日志prelogindex不匹配，则回退指针 follower需要找到第一个不匹配的日志，然后供leader回退
    else
    {
        DPrintf("[OnAppendEntries] log dismatch, nextIndex rollback");
        // 把updatenextindex设置为冲突的term块的起始点
        int conflictIndex = request->prelogindex();
        int conflictTerm = getLogTermFromIndex(conflictIndex);
        while (conflictIndex > lastIncludeSnapshotIndex_ && getLogTermFromIndex(conflictIndex - 1) == conflictTerm)
            conflictIndex--;
        reply.set_updatenextindex(conflictIndex);

        reply.set_term(currentTerm_);
        reply.set_succss(false);
        reply.set_status(LogOutdated); // 这个status没有什么用
    }
}
void Raft::OnAppendEntriesStreamClose(const std::string &nodeId)
{
    int id = std::stoi(nodeId);

    streamServer_[id].AEResponder->Close();
    myAssert(streamServer_[id].AEResponder->Closed(), "[OnAppendEntriesStreamClose] close fail");
    streamServer_[id].AEResponder = nullptr;
    // 这里不需要删除addrToId、idToAddr、peers等对应项
    // 节点可能只是因为分区而下线了，不能删掉相关的注册信息
}
void Raft::OnInstallSnapshotStreamOn(std::unique_ptr<InstallSnapshotResponder> responder, const std::string &nodeId)
{
    // int id = getIdByAddr(peer);
    int id = std::stoi(nodeId);

    DPrintf("[OnInstallSnapshotStreamOn] responder got, from %d", id);
    streamServer_[id].ISResponder = std::move(responder);
    myAssert(streamServer_[id].ISResponder != nullptr, "[OnInstallSnapshotStreamOn] responder save fail");
}
void Raft::OnInstallSnapshotChunk(const RaftRpc::InstallSnapshotArgs *request, const std::string &nodeId)
{
    /*
        message InstallSnapshotArgs
        {
            int32 LeaderId=1;
            int32 Term=2;
            //最后被丢弃的日志索引 用于一致性检查
            int32 LastSnapShotIncludeIndex=3;
            int32 LastSnapShotIncludeTerm=4;
            bytes Data=5; //快照数据
            int32 Offset=6; //当前数据相较于起始快照的偏移量
        }

        message InstallSnapshotReply
        {
            int32 Term=1;
        }
    */
    RaftRpc::InstallSnapshotReply reply;
    int id = std::stoi(nodeId);
    std::lock_guard<std::mutex> lock(mutex_);
    if (currentTerm_ < request->term())
    {
        currentTerm_ = request->term();
        votedFor_ = -1;
        status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
        persist();
    }
    else if (currentTerm_ > request->term())
    {
        reply.set_term(currentTerm_);
        streamServer_[id].ISResponder->SendReply(&reply);
        return; // 不需要后续的接收snapshot步骤了
    }
    status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
    lastResetElectionTime_ = std::chrono::steady_clock::now();

    myAssert(currentTerm_ == request->term(), "[OnInstallSnapshotChunk] currentTerm > request->term()");
    // outdated request
    if (request->lastsnapshotincludeindex() <= lastIncludeSnapshotIndex_)
    {
        return;
    }

    // 如果请求的快照比当前日志的lastLogIndex小，那么需要截断一部分
    if (getLastLogIndex() > request->lastsnapshotincludeindex())
    {
        logs_.erase(logs_.begin(), logs_.begin() + getSlicesIndexFromLogIndex(request->lastsnapshotincludeindex()) + 1);
    }
    else
    {
        logs_.clear();
    }

    // 快照是经过leader确认过的已经提交并应用的日志，所以从节点可以直接应用
    commitIndex_ = std::max(request->lastsnapshotincludeindex(), commitIndex_);
    lastApplied_ = std::max(request->lastsnapshotincludeindex(), lastApplied_);
    lastIncludeSnapshotIndex_ = request->lastsnapshotincludeindex();
    lastIncludeSnapshotTerm_ = request->lastsnapshotincludeterm();

    reply.set_term(currentTerm_);
    ApplyMsg msg;
    msg.SnapshotValid_ = true;
    msg.Snapshot_ = json::parse(request->data());
    msg.SnapshotIndex_ = lastIncludeSnapshotIndex_;
    msg.SnapshotTerm_ = lastIncludeSnapshotTerm_;

    threadPool_->submit([this, msg](){
        pushMsgToKvServer(msg);
    });
    // 应用后作为快照保存
    persister_->save(persistData(), request->data());
}
void Raft::OnInstallSnapshotStreamClose(const std::string &nodeId)
{
    int id = std::stoi(nodeId);
    streamServer_[id].ISResponder->Close();
    myAssert(streamServer_[id].ISResponder->Closed(), "[OnInstallSnapshotStreamClose] close fail");
    streamServer_[id].ISResponder = nullptr;
}
// 发送心跳
void Raft::doHeartBeat()
{
    if (stop_)
        return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (status_ == RaftRpc::RAFT_LEADER)
    {
        DPrintf("[Raft HeartBeat] leader: %d", id_);
        // 对其他所有节点发送AE
        for (auto i = peers_.begin(); i != peers_.end(); i++)
        {
            DPrintf("[Raft HeartBeat] Leader %d to follower %d", id_, i->first);
            myAssert(nextIndex_[i->first] >= 1, format("nextIndex[%d] = {%d}", i->first, nextIndex_[i->first]));
            // 如果follower的日志索引小于压缩的快照index
            // 则需要发送给落后follower快照
            if (nextIndex_[i->first] <= lastIncludeSnapshotIndex_)
            {
                // 这个线程是临时的
                threadPool_->submit([this, i]{
                    leaderSendSnapshot(i->first);
                });
                continue;
            }
            // 构造心跳数据包
            int preLogIndex = -1;
            int preLogTerm = -1;
            // 根据不同节点的nextIndex数组 来获取preIndex
            getPrevLogInfo(i->first, &preLogIndex, &preLogTerm); // 获取当前日志的preIndex和preTerm
            std::shared_ptr<RaftRpc::AppendEntriesArgs> appendEntries = std::make_shared<RaftRpc::AppendEntriesArgs>();
            /*
                int32 Term=1; //消息的任期
                int32 LeaderId=2; //领导者id
                int32 PreLogIndex=3; //前一条日志的索引
                int32 PreLogTerm=4; //前一条日志的任期
                repeated LogEntry Entries=5; //发送的日志 如果为心跳消息，则为空
                int32 LeaderCommit=6; //领导者最新提交索引
            */
            appendEntries->set_term(currentTerm_);
            appendEntries->set_leaderid(id_);
            appendEntries->set_prelogindex(preLogIndex);
            appendEntries->set_prelogterm(preLogTerm);

            // 从nextIndex 到 logs_.size()
            appendEntries->clear_entries();
            if (preLogIndex != lastIncludeSnapshotIndex_)
            {
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < logs_.size(); j++)
                {
                    RaftRpc::LogEntry *entry = appendEntries->add_entries();
                    *entry = logs_[j];
                }
            }
            // 此时刚刚重启 直接传所有log(优化)
            else
            {
                for (const auto &item : logs_)
                {
                    RaftRpc::LogEntry *entry = appendEntries->add_entries();
                    *entry = item;
                }
            }
            appendEntries->set_leadercommit(commitIndex_);

            // 确定日志是从preIndex发到lastIndex的
            int lastLogIndex = getLastLogIndex();
            myAssert(appendEntries->prelogindex() + appendEntries->entries_size() == lastLogIndex, format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                                                                                                          appendEntries->prelogindex(), appendEntries->entries_size(), lastLogIndex));
            const std::shared_ptr<RaftRpc::AppendEntriesReply> reply = std::make_shared<RaftRpc::AppendEntriesReply>();
            /*
                int32 Term=1; //跟随者认为的当前领导者任期
                bool Succss=2; //跟随者是否接收成功
                int32 UpdateNextIndex=3; //根据跟随者的updateNextIndex值，领导者可以调整该跟随者的Next值
                RaftState State=4; //跟随者状态
            */
            reply->set_status(Disconnected);
            // 开临时线程 发送AppendEntries
            // 每个follower一个线程 不会持续很长
            // TODO:如果因nextIndex不匹配 则需要立即重新发送AE
            // 如果follower因为断网等原因没有回复心跳，不需要使用特别的retry，正常用心跳来执行retry即可
            threadPool_->submit(
                // 这里都要复制 线程可能执行很长时间 导致指针、迭代器失效
                [this, i, appendEntries, reply]()
                {
                    // 不能直接Write和Read 因为会涉及到超时的问题
                    int res = SendAppendEntries(i->first, appendEntries, reply);
                    if (res == Retry)
                        DPrintf("[HeartBeat] follower %d log mismatch, will retry next tick", i->first);
                });
        }
        lastResetHeartBeatTime_ = std::chrono::steady_clock::now();
        updateCommitIndex();
    }
}

void Raft::doElection()
{
    if (stop_)
        return;
    std::lock_guard<std::mutex> lock(mutex_);
    // leader不需要选举
    if (status_ != RaftRpc::RaftState::RAFT_LEADER)
    {
        /*
            1. status_ = CANDIDATE
            2. currentTerm ++
            3. vote for self
            4. send RequestVote
            if recevied majority of group's vote, then be leader
        */
        status_ = RaftRpc::RaftState::RAFT_CANDIDATE;
        currentTerm_++;
        votedFor_ = id_;
        persist(); // 凡是状态变更了，就需要持久化

        lastResetElectionTime_ = std::chrono::steady_clock::now();
        auto counter = std::make_shared<std::atomic_int>(1); // 投票结果统计用 因为初始要投票给自己 所以初始值为1
        // 发送RequestVote
        for (auto i = peers_.begin(); i != peers_.end(); i++)
        {
            if (i->first == id_)
                continue;
            int lastLogIndex = -1;
            int lastLogTerm = -1;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
            std::shared_ptr<RaftRpc::RequestVoteArgs> requestVoteArg = std::make_shared<RaftRpc::RequestVoteArgs>();
            /*
                int32 Term=1; //当前任期 跟随者会在发送前自增任期
                int32 CandidateId=2; //当前候选者的id
                //这两项用于让系统拒绝给落后候选者投票
                int32 LastLogTerm=3;
                int32 LastLogIndex=4;
            */
            requestVoteArg->set_term(currentTerm_);
            requestVoteArg->set_candidateid(id_);
            requestVoteArg->set_lastlogterm(lastLogTerm);
            requestVoteArg->set_lastlogindex(lastLogIndex);

            std::shared_ptr<RaftRpc::RequestVoteReply> requestVoteReply = std::make_shared<RaftRpc::RequestVoteReply>();
            requestVoteReply->set_votestate(Disconnected);
            threadPool_->submit([this, i, requestVoteArg, requestVoteReply, counter]()
                                { SendRequestVote(i->first, requestVoteArg, requestVoteReply, counter); });
        }
    }
}
// 会在单独的线程里执行
void Raft::SendRequestVote(int server, std::shared_ptr<RaftRpc::RequestVoteArgs> request, std::shared_ptr<RaftRpc::RequestVoteReply> reply, std::shared_ptr<std::atomic_int> counter)
{
    if(stop_) return;
    if (status_ == RaftRpc::RaftState::RAFT_LEADER)
        return;

    auto start = std::chrono::system_clock::now();
    DPrintf("[SendRequestVote] from %d to %d", id_, server);
    // 设置超时
    grpc::ClientContext context;
    context.AddMetadata("node-id", std::to_string(id_));
    context.set_deadline(start + std::chrono::milliseconds(ElectionTimeOut));
    bool ok = clients_[server]->RequestVote(&context, *request.get(), reply.get());
    if (!ok)
    {
        DPrintf("[SendRequestVote Timeout] leader %d to follower %d", id_, server);
        return;
    }

    // 处理回应 无论是什么类型的回应 都要检查term
    std::lock_guard<std::mutex> lock(mutex_);
    // 收到的term大于当前term
    // 节点变为追随者 voteFor改为-1 term更新
    if (reply->term() > currentTerm_)
    {
        currentTerm_ = reply->term();
        status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
        votedFor_ = -1;
        persist();
        lastResetElectionTime_ = std::chrono::steady_clock::now();
    }
    // 过期的响应不计数
    else if (reply->term() < currentTerm_)
    {
        return;
    }
    myAssert(reply->term() == currentTerm_, "assert {reply->term()==currentTerm_} failed");
    if (!reply->votegranted())
        return; // 没有投票的不计数

    *counter = *counter + 1;
    if (*counter >= peers_.size() + 1)
    {
        *counter = 0;
        status_ = RaftRpc::RaftState::RAFT_LEADER;
        DPrintf("[SendRequestVote] election Success, currentTerm: %d, lastLogIndex %d", currentTerm_, getLastLogIndex());
        int lastlogIndex = getLastLogIndex();
        // 成功选举后，重新初始化状态
        for (auto& peer: peers_)
        {
            auto id = peer.first;
            nextIndex_[id] = lastlogIndex + 1;
            matchIndex_[id] = 0; // 过去的统计信息作废
        }
        persist();
        // 立即做一次心跳
        // TODO:发送no-op日志
        threadPool_->submit([this]()
                            { if(!stop_) doHeartBeat(); });
    }
}
// 会在单独的线程里执行
// 要特别注意幂等操作！
int Raft::SendAppendEntries(int server, std::shared_ptr<RaftRpc::AppendEntriesArgs> request, std::shared_ptr<RaftRpc::AppendEntriesReply> reply)
{
    if(stop_) return Stopped;
    if (status_ != RaftRpc::RaftState::RAFT_LEADER)
        return BeFollower;
    DPrintf("[SendAppendEntries] from %d to %d", id_, server);
    // TODO:同步读写?
    // TODO:设置超时
    grpc::ClientContext context;
    context.AddMetadata("node-id", std::to_string(id_)); // server端只能通过nodeid来相认，因为client端的端口是随机的
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(AppendEntriesTimeOut));
    std::unique_ptr<AppendEntriesStream> stream = clients_[server]->CreateAppendEntriesStream(&context);
    if (!stream->Write(request.get()) || !stream->Read(reply.get()))
    {
        stream->Close();
        return Disconnected;
    }
    stream->Close();

    DPrintf("[SendAppendEntries] success from %d to %d", id_, server);

    std::lock_guard<std::mutex> lock(mutex_); // 加锁 所以实际上各个节点的响应统计是串行的 没有并发问题
    // 只要收到了消息，就需要对term判断
    if (reply->term() > currentTerm_)
    {
        status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
        currentTerm_ = reply->term();
        votedFor_ = -1;
        persist();
        lastResetElectionTime_ = std::chrono::steady_clock::now();
        DPrintf("[SendAppendEntries] leader %d stepdown", id_);
        return BeFollower;
    }
    // 过期响应 作废
    else if (reply->term() < currentTerm_)
    {
        DPrintf("[SendAppendEntries] deprecated node %d", server);
        return Expired;
    }

    // term相等
    myAssert(reply->term() == currentTerm_, "assert {reply->term()==currentTerm_} failed");
    /*
        int32 Term=1; //跟随者认为的当前领导者任期
        bool Succss=2; //跟随者是否接收成功
        int32 UpdateNextIndex=3; //根据跟随者的updateNextIndex值，领导者可以调整该跟随者的Next值
        int32 Status = 4; //跟随者状态
    */
    // follower拒绝心跳 原因1. term不匹配 2.日志前缀匹配失败 这里是后者
    if (!reply->succss())
    {
        // updatenextIndex的默认值为-100 如果不是-100 那么说明follower提供了快速回退，那么直接根据这个值来
        if (reply->updatenextindex() != -100)
        {
            DPrintf("[SendAppendEntires] trigger quick update nextIndex");
            auto suggest = reply->updatenextindex();
            suggest = std::max(1, suggest);
            suggest = std::min(suggest, getLastLogIndex() + 1);

            // 如果回复的值比nextIndex还要大，就设置为nextIndex[server] - 1
            if (suggest > nextIndex_[server])
            {
                nextIndex_[server] = std::min(1, nextIndex_[server] - 1);
            }
            else
            {
                nextIndex_[server] = suggest;
            }
        }
        // 没有回退 直接按照Raft标准来
        else
        {
            nextIndex_[server]--;
        }
        myAssert(nextIndex_[server] >= 1 && nextIndex_[server] <= getLastLogIndex() + 1, "updateNextIndex invalid");
        return Retry;
    }
    else
    {
        // 更新matchIndex和nextIndex
        // 更新matchIndex需要做幂等保护 有两种错误写法
        /*
            // 错误写法1：直接累加
            m_matchIndex[server] += args->entries_size();
            // 错误写法2：直接赋值为“prev + len”（不考虑当前值）
            m_matchIndex[server] = args->prevlogindex() + args->entries_size();

            当网络拥堵时，一个AppendEntriesRpc可能发送多个，如果用法2，那么会导致多余增长 通过max(matchIndex, request->preIndex + request->entries_size)，可以保证只要加过一次后，后续都不会重复增长
            因为如果第一次收到响应后，会右移matchIndex。而后续重复的响应都是基于相同的request的，取max可以屏蔽掉后续响应
            法1也有问题，如果发了旧和新两个AErpc，但先返回的是新，后返回旧，那么会导致旧的值为最终值，所以也是错的
        */
        matchIndex_[server] = std::max(matchIndex_[server], request->prelogindex() + request->entries_size()); // 幂等保护，只能单调增
        nextIndex_[server] = matchIndex_[server] + 1;
        return Success;
    }
}

// Ticker不能退出 因为集群的leader是时刻变化的
// 并且三个ticker每个节点都要有 因为随时都有可能变更leader
void Raft::applierTicker()
{
    while (!stop_)
    {
        if (status_ == RaftRpc::RaftState::RAFT_LEADER)
        {
            DPrintf("[applierTicker] leader %d applied log, appliedIndex: %d", id_, lastApplied_);
        }
        std::vector<ApplyMsg> applylogs;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            applylogs = getApplyLogs();
            if (!applylogs.empty())
                DPrintf("[applierTicker] logs applied, len: %d", applylogs.size());
            // 这里需要加锁 因为installSnapshot也会把日志推到queue里
            for (auto &log : applylogs)
            {
                applyQueue_->push(log);
            }
        }
        sleepNMilliseconds(ApplyInterval);
    }
    DPrintf("[applierTicker] %d stopped", id_);
}
void Raft::electionTimeOutTicker()
{
    while (!stop_)
    {
        if (status_ != RaftRpc::RaftState::RAFT_LEADER)
        {
            std::chrono::nanoseconds sleeptime;
            std::chrono::steady_clock::time_point wakeup;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                wakeup = std::chrono::steady_clock::now();
                /*
                    1. 每次选举都需要获取随机时间 否则会出现活锁问题
                    2. 这里的wakeup是从起始到终点的一个中间节点 用于检测心跳重置
                    3. 不能用相对时间 用相对时间会引入系统调用误差，导致误差累积破坏系统
                */
                sleeptime = getRandomizedElectionTimeOut() + lastResetElectionTime_ - wakeup;
            }
            /*
                有两种情况
                1. wakeup在区间[lastResetElectionTime, ElectionTimeout]内 那么会进入下面第一个分支睡眠
                2. wakeup在区间[ElectionTimeout, +infinty)，此时不会进入睡眠
                两者就算因为系统调用误差超过了timeout时间点也没有误差，因为实际被拖慢的只有doElection（这个无影响）
            */
            // 此时还需要睡 因为心跳时间点未重置
            // 时间的比较单位为毫秒(1s = 1000ms)
            if (std::chrono::duration<double, std::milli>(sleeptime).count() > 1)
            {
                // auto start = std::chrono::steady_clock::now();
                ::usleep(std::chrono::duration_cast<std::chrono::microseconds>(sleeptime).count());
                // auto end = std::chrono::steady_clock::now();
            }
            // 重新循环 因为心跳时间点被重置了
            if (std::chrono::duration<double, std::milli>(lastResetElectionTime_ - wakeup).count() > 0)
            {
                continue;
            }
            else
            {
                DPrintf("[electionTimeOutTicker] Server %d ticker timeout, start election", id_);
                doElection();
            }
        }
        else
            sleepNMilliseconds(HeartBeatTimeOut); // 阻塞多长时间没有标准 就按心跳周期来
    }
    DPrintf("[electionTimeOutTicker] %d stopped", id_);
}
void Raft::leaderHeartBeatTicker()
{
    while (!stop_)
    {
        if (status_ == RaftRpc::RaftState::RAFT_LEADER)
        {
            // 计时方法与electionTicker类似
            // 计时不能用相对定时 要用绝对时间点
            std::chrono::steady_clock::time_point wakeup;
            std::chrono::nanoseconds sleeptime;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                wakeup = std::chrono::steady_clock::now();
                sleeptime = std::chrono::milliseconds(HeartBeatTimeOut) + lastResetHeartBeatTime_ - wakeup;
            }
            if (std::chrono::duration<double, std::milli>(sleeptime).count() > 1)
            {
                ::usleep(std::chrono::duration_cast<std::chrono::microseconds>(sleeptime).count());
            }
            if (std::chrono::duration<double, std::milli>(lastResetHeartBeatTime_ - wakeup).count() > 0)
            {
                continue;
            }
            else
            {
                doHeartBeat();
            }
        }
        // 非Raft的leader 直接睡眠
        else
        {
            sleepNMilliseconds(HeartBeatTimeOut);
        }
    }
    DPrintf("[leaderHeartBeatTicker] %d stopped", id_);
}
// 向指定server号发送快照
void Raft::leaderSendSnapshot(int server)
{
    /*
        message InstallSnapshotArgs
        {
            int32 LeaderId=1;
            int32 Term=2;
            //最后被丢弃的日志索引 用于一致性检查
            int32 LastSnapShotIncludeIndex=3;
            int32 LastSnapShotIncludeTerm=4;
            bytes Data=5; //快照数据
            int32 Offset=6; //当前数据相较于起始快照的偏移量
        }
    */
    if(stop_) return;
    RaftRpc::InstallSnapshotArgs request;
    RaftRpc::InstallSnapshotReply reply;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        request.set_leaderid(id_);
        request.set_term(currentTerm_);
        request.set_lastsnapshotincludeindex(lastIncludeSnapshotIndex_);
        request.set_lastsnapshotincludeterm(lastIncludeSnapshotTerm_);
        request.set_data(persister_->readSnapShot());
        // TODO:需要支持断点重传
        request.set_offset(0);
    }

    grpc::ClientContext context;
    context.AddMetadata("node-id", std::to_string(id_));
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(InstallSnapshotTimeOut));
    auto stream = clients_[server]->CreateInstallSnapshotStream(&context);
    if (!stream->Write(&request) || !stream->Read(&reply))
    {
        stream->Close();
        return;
    }
    stream->Close();

    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 中间释放过锁，可能导致状态变更 需要重新判断
        if (status_ != RaftRpc::RaftState::RAFT_LEADER)
            return;

        if (reply.term() > currentTerm_)
        {
            status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
            currentTerm_ = reply.term();
            votedFor_ = -1;
            persist();
            lastResetElectionTime_ = std::chrono::steady_clock::now();
            return;
        }
        matchIndex_[server] = lastIncludeSnapshotIndex_;
        nextIndex_[server] = matchIndex_[server] + 1;
    }
}

void Raft::updateCommitIndex()
{
    commitIndex_ = lastIncludeSnapshotIndex_;
    // 根据Raft的安全性，只要一个日志提交了，那么它之前的所有日志都是提交的
    int index = getLastLogIndex();
    // 遍历所有日志（从后往前遍历），找到一个大多数节点已提交的日志，那个日志index就是commitIndex
    for (int i = index; i > lastIncludeSnapshotIndex_; i--)
    {
        int sum = 0;
        for (auto& item: peers_)
        {
            if (item.first == id_)
            {
                sum++;
                continue;
            }
            if (matchIndex_[item.first] >= index)
                sum++;
        }
        // 只有当前任期有提交后 才能变更commitIndex（延迟提交）
        if (sum >= peers_.size() / 2 + 1 && getLogTermFromIndex(i) == currentTerm_)
        {
            commitIndex_ = i;
            break;
        }
    }
}

bool Raft::matchLog(int logIndex, int logTerm)
{
    myAssert(logIndex >= lastIncludeSnapshotIndex_ && logIndex <= getLastLogIndex(), "[matchLog] invalid logIndex");
    return getLogTermFromIndex(logIndex) == logTerm;
}

void Raft::persist()
{
    auto data = persistData();
    persister_->saveRaftState(data);
}

bool Raft::whetherVoteFor(int logindex, int term)
{
    int lastTerm = -1;
    int lastIndex = -1;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    // 如果term和请求的term一样，那么比较日志的新旧，如果对方日志不比自己落后，即logIndex>=lastIndex 那么是要投票的
    return term > lastTerm || (term == lastTerm && logindex >= lastIndex);
}

int Raft::getNewCommandIndex()
{
    return getLastLogIndex() + 1;
}

void Raft::getPrevLogInfo(int server, int *preIndex, int *preTerm)
{
    *preIndex = nextIndex_[server] - 1;
    if(*preIndex == lastIncludeSnapshotIndex_)
        *preTerm = lastIncludeSnapshotTerm_;
    else
        *preTerm = logs_[getSlicesIndexFromLogIndex(*preIndex)].logterm();
    /*
        这个逻辑有问题，比较nextIndex和lastIncludeSnapshotIndex是错的，如果nextIndex为1，lastIncludeSnapshotIndex为0，
        那么nextIndex与lastIncludeSnapshotIndex_的分支会跳过，在log里找然后越界。应该用preIndex和lastIncludeSnapshotIndex比
    */
    // 对应节点的nextIndex值为lastIncludeSnapshotIndex 直接返回快照的值
    // if (nextIndex_[server] == lastIncludeSnapshotIndex_)
    // {
    //     *preIndex = lastIncludeSnapshotIndex_;
    //     *preTerm = lastIncludeSnapshotTerm_;
    //     return;
    // }
    // *preIndex = nextIndex_[server] - 1;
    // *preTerm = logs_[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

void Raft::getState(int *term, bool *isLeader)
{
    std::lock_guard<std::mutex> lock(mutex_);
    *term = currentTerm_;
    *isLeader = status_ == RaftRpc::RaftState::RAFT_LEADER;
}

int Raft::getLastLogIndex()
{
    int lastIndex = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&lastIndex, &_);
    return lastIndex;
}
int Raft::getLastLogTerm()
{
    int lastTerm = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&_, &lastTerm);
    return lastTerm;
}
// 要考虑节点刚刚恢复的情况 此时日志为空 lastLogIndex和lastLogTerm就是snapshotIndex和term
void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm)
{
    if (logs_.empty())
    {
        *lastLogIndex = lastIncludeSnapshotIndex_;
        *lastLogTerm = lastIncludeSnapshotTerm_;
    }
    else
    {
        *lastLogIndex = logs_[logs_.size() - 1].logindex();
        *lastLogTerm = logs_[logs_.size() - 1].logterm();
    }
}

int Raft::getLogTermFromIndex(int logIndex)
{
    myAssert(logIndex <= getLastLogIndex() && logIndex >= lastIncludeSnapshotIndex_, "[getLogTermFromIndex] invalid logIndex");
    if (logIndex == lastIncludeSnapshotIndex_)
        return lastIncludeSnapshotIndex_;
    else
        return logs_[getSlicesIndexFromLogIndex(logIndex)].logterm();
}

int Raft::getRaftStateSize()
{
    return persister_->raftStateSize();
}
// 把lastIncludeSnapshotIndex视为-1 它的下一个日志即为0
int Raft::getSlicesIndexFromLogIndex(int logIndex)
{
    myAssert(logIndex <= getLastLogIndex() && logIndex >= lastIncludeSnapshotIndex_, "[getSlicesIndexFromLogIndex] invalid logIndex");
    auto sliceIndex = logIndex - lastIncludeSnapshotIndex_ - 1;
    myAssert(sliceIndex >= -1, "[getSlicesIndexFromLogIndex] logIndex is 0");
    return sliceIndex;
}

std::vector<ApplyMsg> Raft::getApplyLogs()
{
    std::vector<ApplyMsg> applyMsgs;
    myAssert(commitIndex_ <= getLastLogIndex(), "[getApplyLogs] invalid commitIndex");
    while (lastApplied_ < commitIndex_)
    {
        lastApplied_++;
        myAssert(lastApplied_ == logs_[getSlicesIndexFromLogIndex(lastApplied_)].logindex(), "[getApplyLogs] not equal with lastApplied and logs_[lastApplied].logindex()");
        ApplyMsg applyMsg;
        applyMsg.CommandValid_ = true;
        applyMsg.Command_ = logs_[getSlicesIndexFromLogIndex(lastApplied_)].command();
        applyMsg.CommandIndex_ = lastApplied_;
        applyMsg.SnapshotValid_ = false; // 不是snapshot

        applyMsgs.emplace_back(applyMsg);
    }
    return applyMsgs;
}

void Raft::pushMsgToKvServer(ApplyMsg msg)
{
    applyQueue_->push(msg);
}
// deserialize by json
void Raft::readPersist(std::string v)
{
    if (v.empty())
        return;
    json state = json::parse(v);
    if (state.contains("currentTerm") && state.at("currentTerm").is_number())
        currentTerm_ = state.at("currentTerm");
    else
        DPrintf("[readPersist] currentTerm parse error");
    if (state.contains("votedFor") && state.at("votedFor").is_number())
        votedFor_ = state.at("votedFor");
    else
        DPrintf("[readPersist] votedFor parse error");

    if (state.contains("lastIncludeSnapshotIndex") && state.at("lastIncludeSnapshotIndex").is_number())
        lastIncludeSnapshotIndex_ = state.at("lastIncludeSnapshotIndex");
    else
        DPrintf("[readPersist] lastIncludeSnapshotIndex parse error");

    if (state.contains("lastIncludeSnapshotTerm") && state.at("lastIncludeSnapshotTerm").is_number())
        lastIncludeSnapshotTerm_ = state.at("lastIncludeSnapshotTerm");
    else
        DPrintf("[readPersist] lastIncludeSnapshotTerm parse error");

    if (state.contains("logs") && state.at("logs").is_array())
    {
        for (auto log : state.at("logs"))
        {
            RaftRpc::LogEntry entry;
            if (log.contains("logTerm") && log.at("logTerm").is_number())
                entry.set_logterm(log.at("logTerm"));
            if (log.contains("logIndex") && log.at("logIndex").is_number())
                entry.set_logindex(log.at("logIndex"));
            if (log.contains("command") && log.at("command").is_number())
                entry.set_command(log.at("command"));

            logs_.emplace_back(entry);
        }
        myAssert(state.at("logs").size() == logs_.size(), "[readPersist] the size of logs json and object logs_ are not equal");
    }
    else
        myAssert(false, "[readPersist] logs parse error");
}

std::string Raft::persistData()
{
    std::string v;
    json j =
        {
            {"currentTerm", currentTerm_},
            {"votedFor", votedFor_},
            {"lastIncludeSnapshotIndex", lastIncludeSnapshotIndex_},
            {"lastIncludeSnapshotTerm", lastIncludeSnapshotTerm_}};

    json temp = json::array();
    for (int i = 0; i < logs_.size(); i++)
    {
        json entry =
            {
                {"logTerm", logs_[i].logterm()},
                {"logIndex", logs_[i].logindex()},
                {"command", logs_[i].command()}};
        temp.emplace_back(entry);
    }
    j["logs"] = temp;
    return j.dump(4);
}

const std::string Raft::getAddrById(int id)
{
    auto it = idToAddr_.find(id);
    myAssert(it != idToAddr_.end(), "[getAddrById] can't get addr from addrToId_");
    return it->second;
}