#pragma once
//配置
const bool Debug = true;
const int DebugFactor = 1; //用于控制超时时间
const int HeartBeatTimeOut = 25 * DebugFactor; //ms 心跳超时
const int ElectionTimeOut = 500 * DebugFactor; //ms 选举超时
const int ApplyInterval = 10 * DebugFactor; //Raft节点apply日志的间隔
//选举超时时间区间 超时时间为均匀分布
const int MinRandomizedElectionTime = 300 * DebugFactor; //ms
const int MaxRandomizedElectionTime = 500 * DebugFactor; //ms
const int ConsensusTimeOut =  500 * DebugFactor;