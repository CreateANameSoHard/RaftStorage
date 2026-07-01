#pragma once
#include <string>

#include "../include/json.hpp"

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
    bool CommandValid_;   // 是否为一般命令
    std::string Command_; // 命令内容
    int CommandIndex_;    // 命令索引
    bool SnapshotValid_;  // 是否以作为快照保存
    nlohmann::json Snapshot_; // 快照内容
    int SnapshotTerm_; // 快照term
    int SnapshotIndex_; // 快照索引
};