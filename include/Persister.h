#pragma once
#include <fstream>
#include <shared_mutex>
#include <atomic>
#include <string>
//持久化器 将 Raft 节点的状态（term、voteFor、日志等）和快照（压缩后的状态机数据）保存到磁盘
class Persister
{
    public:
        explicit Persister(int placeholder);
        ~Persister();

        void save(const std::string& raftState, const std::string& snapShot);
        std::string readSnapShot();

        long long raftStateSize();
        std::string readRaftState();
        void saveRaftState(const std::string& v);

    private:
        void clearRaftState();
        void clearSnapShot();
        void clearRaftStateAndSnapShot();

        std::shared_mutex mutex_;
        std::string raftState_;
        std::string snapShot_;
        //raft状态文件名
        const std::string raftStateFileName_;
        std::ofstream raftStateWriter_;
        //快照文件名
        const std::string snapShotFileName_;
        std::ofstream snapShotWriter_;
        //保存rafeState大小 避免每次都读取文件来获取大小
        std::atomic_llong raftStateSize_; 
};