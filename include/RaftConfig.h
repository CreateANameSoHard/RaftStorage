#pragma once
#include <unordered_map>
#include <string>

//Raft节点地址
class RaftConfig
{
public:
    static RaftConfig& GetInstance();

    void load(std::string path); //读取配置文件内容到config_对象
    std::string get(std::string& key);
private:
    RaftConfig() = default;
    ~RaftConfig() = default;
    RaftConfig(const RaftConfig &) = delete;
    RaftConfig &operator=(const RaftConfig &) = delete;

    void trim(std::string& str);

    std::unordered_map<std::string, std::string> config_;
};