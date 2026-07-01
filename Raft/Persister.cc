#include "../include/Persister.h"
#include "../include/Util.h"



Persister::Persister(int placeholder)
    : raftStateFileName_("RaftStatePersist" + std::to_string(placeholder) + ".json"),
    snapShotFileName_("SnapShotPersist" + std::to_string(placeholder) + ".json"),
    raftStateSize_(0),
    snapshotSize_(0)
{
    //试着打开两个文件 如果有一个打不开则flag为false
    bool fileOpenFlag = true;
    std::fstream file(raftStateFileName_, std::ios_base::out | std::ios_base::trunc);
    if(file.is_open())
        file.close();
    else
        fileOpenFlag = false;

    file = std::fstream(snapShotFileName_, std::ios_base::out | std::ios_base::trunc);
    if(file.is_open())
        file.close();
    else
        fileOpenFlag = false;
    
    if(!fileOpenFlag)
        DPrintf("[func-Persister::Persister] file open error");
    
    //打开文件 绑定到流对象
    //两个都是trunc 一个Raft节点只需要保存最新的一个snapshot 所以用trunc是合理的
    raftStateWriter_.open(raftStateFileName_, std::ios_base::trunc);
    snapShotWriter_.open(snapShotFileName_, std::ios_base::trunc);
}

Persister::~Persister()
{
    if(raftStateWriter_.is_open())
        raftStateWriter_.close();
    if(snapShotWriter_.is_open())
        snapShotWriter_.close();

    raftStateWriter_.flush();
    snapShotWriter_.flush();
}
//会把状态文件和快照文件覆盖
void Persister::save(const std::string& raftState, const std::string& snapShot)
{
    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    //清理对象 写入文件持久化
    clearRaftStateAndSnapShot();
    json raftState_json = json::parse(raftState);
    raftStateWriter_ << raftState;
    raftStateSize_ += raftState_json.size();

    json snapshot_json = json::parse(snapShot);
    snapShotWriter_ << snapShot;
    snapshotSize_ += snapshot_json.size();

    raftStateWriter_.flush();
    snapShotWriter_.flush();
}

std::string Persister::readRaftState()
{
    std::shared_lock<std::shared_mutex> read_lock(mutex_);

    std::fstream istream(raftStateFileName_, std::ios_base::in);
    //打开失败
    if(!istream.good())
        return "";

    try
    {
        json state;
        istream >> state;  // 可能抛出 parse_error
        istream.close();
        return state.dump(4);  // 返回格式化的 JSON 字符串
    }
    catch (const nlohmann::json::parse_error& e)
    {
        // 文件内容为空或格式错误，返回空字符串
        DPrintf("[Persister] readRaftState parse error: %s", e.what());
        return "";
    }
}

void Persister::saveRaftState(const std::string& v)
{
    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    clearRaftState();
    json j = json::parse(v);
    raftStateWriter_ << j;
    // 如果长期持有流对象 就应该写了后刷新缓冲 更何况这是Raft的场景 有必要立即写到磁盘
    raftStateWriter_.flush();
    raftStateSize_ += j.size();
}

long long Persister::raftStateSize()
{
    return raftStateSize_.load(std::memory_order_relaxed);
}
long long Persister::snapshotSize()
{
    return snapshotSize_.load(std::memory_order_relaxed);
}
// only use for json
std::string Persister::readSnapShot()
{
    std::shared_lock<std::shared_mutex> read_lock(mutex_);
    //暂时关闭快照输出 因为要读快照了
    if(snapShotWriter_.is_open()) snapShotWriter_.close();
    //用DEFER来让读完后再重新打开
    DEFER
    {
        snapShotWriter_.open(snapShotFileName_, std::ios_base::app);
    };

    std::fstream istream(snapShotFileName_, std::ios_base::in);
    if(!istream.good()) return "";
    json snapshot;
    istream >> snapshot;
    istream.close();
    return snapshot.dump(4);
}
//重新打开RaftStateFile
void Persister::clearRaftState()
{
    raftStateSize_ = 0;
    if(raftStateWriter_.is_open()) raftStateWriter_.close();
    raftStateWriter_.open(raftStateFileName_, std::ios_base::trunc | std::ios_base::out);
}

void Persister::clearSnapShot()
{
    if(snapShotWriter_.is_open()) snapShotWriter_.close();
    snapShotWriter_.open(snapShotFileName_, std::ios_base::trunc | std::ios_base::out);
}

void Persister::clearRaftStateAndSnapShot()
{
    clearRaftState();
    clearSnapShot();
}