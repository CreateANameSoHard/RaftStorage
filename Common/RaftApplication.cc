#include <unistd.h>

#include "../include/RaftApplication.h"

RaftApplication& RaftApplication::GetInstance()
{
    static RaftApplication app;
    return app;
}
//TODO:
void RaftApplication::Init(int argc, char* argv[])
{
    int o;
    const char* optstr = "c:o::"; //-c 配置文件 -o 日志存放目录 默认为当前路径
    while(-1 != (o = ::getopt(argc, argv, optstr)))
    {

    }
}