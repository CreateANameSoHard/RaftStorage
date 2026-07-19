#pragma once
#include <string>
//配置
const bool Debug = true;
const int DebugFactor = 2; //用于控制超时时间
const int AppendEntriesTimeOut = 50 * DebugFactor; //ms AppendEntries通信超时
const int InstallSnapshotTimeOut = 50 * DebugFactor;
const int HeartBeatTimeOut = 25 * DebugFactor; //ms 心跳超时
const int PreVoteTimeOut = 500 * DebugFactor;
const int ElectionTimeOut = 500 * DebugFactor; //ms 选举超时
const int ApplyInterval = 10 * DebugFactor; //Raft节点apply日志的间隔

const int SnapshotThreshold = 500 * DebugFactor;
// const int SnapshotCheckInterval = 100 * DebugFactor;

//选举超时时间区间 超时时间为均匀分布
const int MinRandomizedElectionTime = 300 * DebugFactor; //ms
const int MaxRandomizedElectionTime = 500 * DebugFactor; //ms
const int ConsensusTimeOut =  500 * DebugFactor;

const std::string delimiter = ":";
const int maxLevel = 100;
const std::string dumpPath = ".";