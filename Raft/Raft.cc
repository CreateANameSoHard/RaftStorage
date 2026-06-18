#include <cmath>
#include <thread>

#include "../include/Raft.h"
#include "../include/Persister.h"
#include "../include/Config.h"
// 网络状态
static constexpr int Disconnected = 0;
static constexpr int Success = 1;

// Vote状态
static constexpr int Killed = 0;
static constexpr int Voted = 1;
static constexpr int Expired = 2;
static constexpr int Normal = 3;

// peers为所有节点的id
Raft::Raft(std::vector<std::shared_ptr<grpc::Channel>> peers, int id, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyQueue)
    : peers_(peers),
      persister_(persister),
      id_(id),
      currentTerm_(0),
      votedFor_(-1),
      commitIndex_(0),
      lastApplied_(0),
      status_(RaftRpc::RaftState::RAFT_FOLLOWER),
      applyQueue_(applyQueue),
      lastResetElectionTime_(std::chrono::steady_clock::now()),
      lastResetHeartBeatTime_(std::chrono::steady_clock::now()),
      lastIncludeSnapshotIndex_(0),
      lastIncludeSnapshotTerm_(0)
{
    // std::lock_guard<std::mutex> lock(mutex_); Raft节点初始化好像没有必要加锁

    server_ = std::make_unique<RaftServer>("127.0.0.1", "9191", this);
    for (int i = 0; i < peers_.size(); i++)
    {
        if (i == id_)
            continue;
        auto client = std::make_unique<RaftClient>(peers_[i]);
        clients_.emplace(i + 1, client);

        FollowerSession session;
        session.context = std::make_unique<grpc::ClientContext>();
        // session.context->set_deadline();
        session.stream = clients_[i]->CreateAppendEntriesStream(session.context.get());
        appendEntriesClient_.emplace(session);

        matchIndex_.push_back(0);
        nextIndex_.push_back(0);
    }

    // 崩溃后需要读已持久化的状态
    // 会把持久化的内容读到对象里 包括：currentTerm, votedFor, logs,lastIncludeSnapshotIndex lastIncludeSnapshotTerm
    readPersist(persister_->readRaftState());
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
    leaderHeartBeatTickerThread_->detach();
    // 选举线程 用于超时后发起选举
    electionTimeOutTickerThread_ = std::make_unique<std::thread>([this]()
                                                                 { this->electionTimeOutTicker(); });
    electionTimeOutTickerThread_->detach();
    // 应用线程 用于定时应用日志
    applierTickerThread_ = std::make_unique<std::thread>([this]()
                                                         { this->applierTicker(); });
    applierTickerThread_->detach();
}

Raft::~Raft()
{
    if (leaderHeartBeatTickerThread_->joinable())
        leaderHeartBeatTickerThread_->join();
    if (electionTimeOutTickerThread_->joinable())
        electionTimeOutTickerThread_->join();
    if (applierTickerThread_->joinable())
        applierTickerThread_->join();
    for (int i = 0; i < appendEntriesClient_.size(); i++)
    {
        appendEntriesClient_[i].stream->Close();
    }

    // TODO: 持久化日志？
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
void OnRequestVote(const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply)
{
}
void OnAppendEntriesStreamOn(std::unique_ptr<AppendEntriesResponder> responder)
{
}
void OnAppendEntries(const RaftRpc::AppendEntriesArgs *request)
{
}
void OnAppendEntriesStreamClose()
{
}
void OnInstallSnapshotStreamOn(std::unique_ptr<InstallSnapshotResponder> responder)
{
}
void OnInstallSnapshotChunk(const RaftRpc::InstallSnapshotArgs *request)
{
}
void OnInstallSnapshotStreamClose()
{
}
// 发送心跳
void Raft::doHeartBeat()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (status_ == RaftRpc::RAFT_LEADER)
    {
        DPrintf("[Raft HeartBeat] leader: %d", id_);
        std::shared_ptr<int> counter = std::make_shared<int>(1); // 统计心跳结果用 初始值为1是因为本机算心跳成功
        // 对其他所有节点发送AE
        for (int i = 0; i < appendEntriesClient_.size(); i++)
        {
            DPrintf("[Raft HeartBeat] Leader %d to follower %d", id_, i);
            myAssert(nextIndex_[i] >= 1, format("nextIndex[%d] = {%d}", i, nextIndex_[i]));
            // 如果follower的日志索引小于压缩的快照index
            // 则需要发送给落后follower快照
            if (nextIndex_[i] <= lastIncludeSnapshotIndex_)
            {
                // 这个线程是临时的
                std::thread t(&Raft::leaderSendSnapshot, this, i);
                t.detach();
                continue;
            }
            // 构造心跳数据包
            int preLogIndex = -1;
            int preLogTerm = -1;
            // 根据不同节点的nextIndex数组 来获取preIndex
            getPrevLogInfo(i, &preLogIndex, &preLogTerm); // 获取当前日志的preIndex和preTerm
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

            // getSlicesIndexFromLogIndex 到 logs_.size()
            appendEntries->clear_entries();
            if (preLogIndex != lastIncludeSnapshotIndex_)
            {
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < logs_.size(); j++)
                {
                    RaftRpc::LogEntry *entry = appendEntries->add_entries();
                    *entry = logs_[j];
                }
            }
            // 此时刚刚重启 直接传所有log(此时log刚从持久化文件里读出)
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
            std::thread HeartBeatThread(
                [&]()
                {
                    // 不能直接Write和Read 因为会涉及到超时的问题
                    // appendEntriesClient_[i].stream->Write(appendEntries.get());
                    // appendEntriesClient_[i].stream->Read(reply.get());
                    SendAppendEntries(i, appendEntries, reply, counter);
                });
            HeartBeatThread.detach();
        }
        lastResetHeartBeatTime_ = std::chrono::steady_clock::now();
    }
}

void Raft::doElection()
{
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
        auto counter = std::make_shared<int>(1); // 投票结果统计用 因为初始要投票给自己 所以初始值为1
        // 发送RequestVote
        for (int i = 0; i < peers_.size(); i++)
        {
            if (i == id_)
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
            requestVoteArg->set_lastlogterm(lastIncludeSnapshotTerm_);
            requestVoteArg->set_lastlogindex(lastIncludeSnapshotIndex_);

            std::shared_ptr<RaftRpc::RequestVoteReply> requestVoteReply = std::make_shared<RaftRpc::RequestVoteReply>();
            requestVoteReply->set_votestate(Disconnected);
            // 启动临时线程
            std::thread requestVoteThread(
                [&]()
                {
                    SendRequestVote(i, requestVoteArg, requestVoteReply, counter);
                });
            requestVoteThread.detach();
        }
    }
}
// 会在单独的线程里执行
void Raft::SendRequestVote(int server, std::shared_ptr<RaftRpc::RequestVoteArgs> request, std::shared_ptr<RaftRpc::RequestVoteReply> reply, std::shared_ptr<int> counter)
{
    if (status_ == RaftRpc::RaftState::RAFT_LEADER)
        return;

    auto start = std::chrono::steady_clock::now();
    DPrintf("[SendRequestVote] from %d to %d", id_, server);
    // 设置超时
    grpc::ClientContext context;
    context.set_deadline(start + std::chrono::milliseconds(ElectionTimeOut));
    bool ok = clients_[server]->RequestVote(&context, *request.get(), reply.get());
    if (!ok)
        return;

    // 处理回应 无论是什么类型的回应 都要检查term
    std::lock_guard<std::mutex> lock(mutex_);
    // 收到的term大于当前term
    // 节点变为追随者 voteFor改为-1 term更新
    if (reply->term() > currentTerm_)
    {
        currentTerm_ = reply->term();
        status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
        votedFor_ = -1;
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
        for (int i = 0; i < nextIndex_.size(); i++)
        {
            nextIndex_[i] = lastlogIndex + 1;
            matchIndex_[i] = 0; // 过去的统计信息作废
        }
        persist();
        // 立即做一次心跳
        // TODO:发送no-op日志
        std::thread HeartBeatThread(
            [&]()
            {
                doHeartBeat();
            });
    }
}
// 会在单独的线程里执行
// 要特别注意幂等操作！
void Raft::SendAppendEntries(int server, std::shared_ptr<RaftRpc::AppendEntriesArgs> request, std::shared_ptr<RaftRpc::AppendEntriesReply> reply, std::shared_ptr<int> counter)
{
    if (status_ != RaftRpc::RaftState::RAFT_LEADER)
        return;
    DPrintf("[SendAppendEntries] from %d to %d", id_, server);
    // TODO:同步读写?
    // TODO:设置超时
    appendEntriesClient_[server].stream->Write(request.get());
    appendEntriesClient_[server].stream->Read(reply.get());
    if (reply->status() == Disconnected)
        return;
    DPrintf("[SendAppendEntries] success from %d to %d", id_, server);

    std::lock_guard<std::mutex> lock(mutex_); // 加锁 所以实际上各个节点的响应统计是串行的 没有并发问题
    // 在appendEntries里也要检查term 因为leader可能是旧的、过时的
    if (reply->term() > currentTerm_)
    {
        status_ = RaftRpc::RaftState::RAFT_FOLLOWER;
        currentTerm_ = reply->term();
        votedFor_ = -1;
        DPrintf("[SendAppendEntries] leader %d stepdown", id_);
        return;
    }
    // 过期响应 作废
    else if (reply->term() < currentTerm_)
    {
        DPrintf("[SendAppendEntries] deprecated node %d", server);
        return;
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
    }
    else
    {
        *counter = *counter + 1;
        // 更新matchIndex和nextIndex
        // 更新matchIndex需要做幂等保护 有两种错误写法
        /*
            // 错误写法1：直接累加
            m_matchIndex[server] += args->entries_size();
            // 错误写法2：直接赋值为“prev + len”（不考虑当前值）
            m_matchIndex[server] = args->prevlogindex() + args->entries_size();

            当网络拥堵时，一个AppendEntriesRpc可能发送多个，如果用法2，那么会导致多余增长
            法1也有问题，如果发了旧和新两个AErpc，但先返回的是新，后返回旧，那么会导致旧的值为最终值，所以也是错的
        */
        matchIndex_[server] = std::max(matchIndex_[server], request->prelogindex() + request->entries_size()); // 幂等保护，只能单调增
        nextIndex_[server] = matchIndex_[server] + 1;
        if (*counter >= peers_.size() / 2 + 1)
        {
            // 此时可以commit
            *counter = 0;
            // 非心跳 记录一下日志
            if (request->entries_size() > 0)
                DPrintf("[SendAppendEntries]%d to %d log commit success: log size: %d", request->entries_size());
            // 非心跳 并且提交的最后一个日志的term等于currentterm（leader只能提交当前term的日志 其他term的日志管不了）
            if (request->entries_size() > 0 && request->entries(request->entries_size() - 1).logterm() == currentTerm_)
            {
                commitIndex_ = std::max(commitIndex_, request->prelogindex() + request->entries_size());
                DPrintf("[SendAppendEntries] commit index forward %d", request->entries_size());
            }
            myAssert(commitIndex_ <= getLastLogIndex(), "assert commitIndex <= lastLogIndex failed");
        }
    }
}

void Raft::applierTicker()
{
    //TODO:析构需要退出线程 可以添加一个flag
    while(true)
    {
        if(status_ == RaftRpc::RaftState::RAFT_LEADER)
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                std::vector<ApplyMsg> applylogs = getApplyLogs();
                if(!applylogs.empty()) DPrintf("[applierTicker] logs applied, len: %d", applylogs.size());
                for(auto& log: applylogs)
                {
                    applyQueue_->push(log);
                }
            }
            sleepNMilliseconds(ApplyInterval);
        }
        else
        {
            // 避免循环break
            sleepNMilliseconds(ApplyInterval);
            break;
        }
    }
}
void Raft::electionTimeOutTicker()
{
    //TODO:析构需要退出线程 可以添加一个flag
    while(true)
    {
        if(status_ != RaftRpc::RaftState::RAFT_LEADER)
        {

        }
    }
}
void leaderHeartBeatTicker()
{
}
void leaderSendSnapshot(int server)
{
}