#pragma once
#include <arpa/inet.h>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <functional>
#include <condition_variable>
#include <unistd.h>
#include <queue>
#include <sys/socket.h>

#include "KVServer.pb.h"
#include "Config.h"
// 模拟GoLang的Defer关键字 让func在对象析构时执行
// 作用域保护器 ScopeGuard
template <class F>
class DeferClass
{
public:
    // 完美转发
    DeferClass(F &&func)
        : func_(std::forward<F>(func))
    {
    }
    DeferClass(const F &func)
        : func_(func)
    {
    }
    ~DeferClass() { func_(); }

    DeferClass(const DeferClass &) = delete;
    DeferClass &operator=(const DeferClass &) = delete;

private:
    F func_;
};

#define _CONCAT(a, b) a##b
// 创建一个DeferClass对象 对象的func_为空调用对象
// 对象名为defer_placeholder+line 如defer_placeholer13
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()
// 先去定义再定义 安全一点
#undef DEFER
/*
    DEFER用法：
    假如需要用DEFER来关锁 或者用DEFER来释放资源
    DEFER
    {
        lock.unlock();
        delete resource;
    };
    这些都会在函数栈最后释放前执行
*/
#define DEFER _MAKE_DEFER_(__LINE__)
//Debug printf
void DPrintf(const char *format, ...);
// 对断言的封装 增强判断
void myAssert(bool condition, std::string msg = "Assertion failed!");
// 根据模板字符串输出
// 与普通的snprintf相比 添加了一个buf大小判断的步骤 保证format大小没问题
template <typename... Args>
std::string format(const char *formatStr, Args... args)
{
    // 返回formatStr+\0的字节数
    //  当buf和size参数为null和0时 snprintf是不会写入的，只会返回字符串的长度
    int size_s = ::snprintf(NULL, 0, formatStr, args...) + 1;
    if (size_s < 0)
        throw std::runtime_error("Error during formatting");
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    snprintf(buf.data(), buf.size(), formatStr, args...);
    return std::string(buf.data(), buf.data() + size - 1); //-1是减去\0
}

// 获取选举超时时间electionTimeout
std::chrono::milliseconds getRandomizedElectionTimeOut();
// sleep N微秒
void sleepNMilliseconds(int N);

// 有锁队列
template <typename T>
class LockQueue
{
public:
    LockQueue() = default;
    ~LockQueue() = default;

    void push(const T &v)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(v);
        cv_.notify_one();
    }
    T pop()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&]()
                 { return !queue_.empty(); });
        T v = queue_.front();
        queue_.pop();
        return v;
    }
    // 超时弹出 即限定超时时间 如果时间过了但队列还为空 则返回false 防止因队列为空而阻塞
    bool timeoutPop(int timeout, T *res)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        auto now = std::chrono::steady_clock::now();
        auto deadline = now + std::chrono::milliseconds(timeout);

        // 如果没有超时且队列为空 忙等
        while (queue_.empty())
        {
            if (cv_.wait_until(lock, deadline) == std::cv_status::timeout)
            {
                return false;
            }
            else
            {
                continue;
            }
        }
        T v = queue_.front();
        queue_.pop();
        *res = v;
        return true;
    }

private:
    std::mutex mutex_;
    std::queue<T> queue_;
    std::condition_variable cv_;
};

// KVServer传给Raft的操作 相当于上层KVServer传给下层Raft节点的日志内容
class Op
{
public:
    std::string operation; // Get Put Del等
    std::string key;
    std::string value;
    std::string clientId; // 客户端id
    int requestId;        // 请求id 按照raft一致性要求 请求id需要持久化

    // 序列化为字符串
    std::string asString() const
    {
        ServerRpc::Op op;
        op.set_operation(operation);
        op.set_key(key);
        op.set_value(value);
        op.set_clientid(clientId);
        op.set_requestid(requestId);
        std::string output;
        op.SerializeToString(&output);
        return output;
    }

    bool parseFromString(std::string &str)
    {
        ServerRpc::Op op;
        if (op.ParseFromString(str))
        {
            this->operation = op.operation();
            this->key = op.key();
            this->value = op.value();
            this->clientId = op.clientid();
            this->requestId = op.requestid();
            return true;
        }
        else
        {
            std::cout << "something wrong while parseFromString in protobuf" << std::endl;
            return false;
        }
    }

    friend std::ostream &operator<<(std::ostream &os, const Op &op)
    {
        os << "[Op] Operation: " << op.operation << "Key: " << op.key << "Value: " << op.value << "ClientId: " << op.clientId << "RequestId: " << std::to_string(op.requestId);
        return os;
    }
};

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

// bool isPortUseable(uint16_t port); //端口是否可用
// bool getUseablePort(short& port); //获取可用端口