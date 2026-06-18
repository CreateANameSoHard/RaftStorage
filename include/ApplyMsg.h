#pragma once
#include <string>

// Raft节点和KVServer的通信数据
class ApplyMsg
{
public:
    ApplyMsg()
        : CommandValid_(false),
          Command_(),
          CommandIndex_(-1),
          SnapshotValid_(false),
          Snapshot_(),
          SnapshotTerm_(-1),
          SnapshotIndex_(-1)
    {
    }

private:
    bool CommandValid_;   // 命令有效性
    std::string Command_; // 命令
    int CommandIndex_;    // 命令索引
    bool SnapshotValid_;  // 是否以作为快照保存
    std::string Snapshot_;
    int SnapshotTerm_;
    int SnapshotIndex_;
};