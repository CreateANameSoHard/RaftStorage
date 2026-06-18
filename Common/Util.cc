#include <stdarg.h>
#include <random>
#include <thread>

#include "../include/Util.h"
#include "../include/Config.h"

void DPrintf(const char *format, ...)
{
    if(Debug)
    {
        va_list list;
        va_start(list, format);

        time_t now = time(nullptr);
        tm* now_tm = ::localtime(&now);
        ::printf("[%d/%d/%d %d:%d:%d] ", now_tm->tm_year + 1900, now_tm->tm_mon + 1, now_tm->tm_mday, now_tm->tm_hour, now_tm->tm_min, now_tm->tm_sec);
        ::vprintf(format, list); //后面的参数按照format格式来
        ::printf("\n");
        //va_arg(list, int)
        va_end(list);
    }
}

void myAssert(bool condition, std::string message)
{
    if(!condition)
    {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}
//用均匀分布来实现选举超时时间随机化
std::chrono::milliseconds getRandomizedElectionTimeOut()
{
    std::random_device rd; //随机数引擎
    std::mt19937 rng(rd()); //mt19937算法
    std::uniform_int_distribution<int> distribution(MinRandomizedElectionTime, MaxRandomizedElectionTime);
    return std::chrono::milliseconds(distribution(rng));
}
void sleepNMilliseconds(int N)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(N));
}