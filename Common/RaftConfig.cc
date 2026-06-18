#include <fstream>
#include <stdexcept>

#include "../include/RaftConfig.h"

RaftConfig &RaftConfig::GetInstance()
{
    static RaftConfig config;
    return config;
}

void RaftConfig::load(std::string path)
{
    std::ifstream reader(path);
    if (!reader.is_open())
        throw std::runtime_error("open file error");
    std::string line;
    while (std::getline(reader, line))
    {
        trim(line);
        if (line[0] == '/' && line[1] == '/')
            continue;
        auto equal = line.find("=");
        if (equal == line.npos)
            continue; // 没有等号下一行
        auto key = line.substr(0, equal);
        trim(key);
        auto value = line.substr(equal + 1, line.size());
        trim(value);
        config_.emplace(key, value);
    }
}

std::string RaftConfig::get(std::string& key)
{
    auto it = config_.find(key);
    if(it == config_.end()) return "";
    return config_[key];
}

void RaftConfig::trim(std::string &str)
{
    auto index = str.find_first_not_of(" ");
    if (index == str.npos)
        return;
    else
    {
        str = str.substr(index, str.size() - index);
        index = str.find_last_not_of(" ");
        str = str.substr(0, index + 1);
    }
}