#pragma once
#include <vector>
#include <unordered_map>
#include <string>

#include "RaftConfig.h"

// 读取程序输入参数、加载Raft节点地址
class RaftApplication
{
public:
    static RaftApplication &GetInstance();
    static void Init(int argc, char *argv[]);

    static RaftConfig &getConfig();

private:
    RaftApplication() = default;
    ~RaftApplication() = default;
    RaftApplication(const RaftApplication &) = delete;
    RaftApplication &operator=(const RaftApplication &) = delete;

    static RaftConfig config_;
};