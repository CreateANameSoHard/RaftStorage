#include "Raft.h"
#include "SkipLists.h"

class StorageBase
{
    virtual ~StorageBase() = default;
    virtual std::string genSnapshot() = 0;
    virtual void getSnapshot() = 0;
};
// skiplist KV server
class KVServer : public StorageBase
{
public:
    KVServer(std::string ip, std::string port,
             std::unordered_map<int, const std::string> idToAddr,
             int id, std::string backup);
    ~KVServer();

    void Get(Op op, std::string* value, bool* exit);
    void Append(Op op);
    void Put(Op op);

private:
    std::shared_ptr<LockQueue<ApplyMsg>> applyQue_;
    std::shared_ptr<Raft> raft_;
    std::shared_ptr<list<std::string, std::string>> skipList_;

    std::string ip_;
    std::string port_;
};