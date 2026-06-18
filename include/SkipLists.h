#pragma once
#include <string>
#include <mutex>
#include <fstream>
#include <sstream>
#include <ctime>
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <unistd.h>

//跳表节点
template <class Key, class Value>
class Node
{
public:
    Node(const Key &key, const Value &value, int level)
        : key_(key),
          value_(value),
          level_(level)
    {
        forward_ = new Node<Key, Value> *[level + 1];
        std::fill(forward_, forward_ + level + 1, nullptr);
    }
    ~Node()
    {
        delete[] forward_;
    }

    Key getKey() const { return key_; }
    Value getValue() const { return value_; }
    void setValue(const Value &v) { value_ = v; }

    Node<Key, Value> **forward_; // 二级指针 指向节点的每一层的下一个节点
    int level_;                  // 节点的最高层级

private:
    Key key_;
    Value value_;
};

template <class Key, class Value>
class list
{
public:
    //maxlevel: 跳表的最大高度
    //path:持久化路径
    //delimiter:数据分隔符
    list(int maxlevel, std::string path, std::string delimiter = ":");
    ~list();

    int getRandomLevel();                          // 利用随机数生成器来设置节点的最高层级
    Node<Key, Value> *createNode(Key, Value, int); // 创建节点
    bool insert(const Key&,const Value&);          // 插入节点
    void display() const;                          // 输出跳表
    bool find(const Key&) const;                          // 查找节点
    bool erase(const Key&);                               // 删除节点
    void clear(Node<Key, Value> *);                // 递归删除节点
    void dump();                                   // 持久化
    void load();                                   // 加载文件

    int size() const; // 获取跳表大小
private:
    bool isValidString(const std::string &str);

    int maxLevel_;              // 跳表的最大层级
    int curLevel_;              // 跳表现在的最高层级
    Node<Key, Value> *header_; // 头节点 跳表只有一个头节点

    // 用于持久化
    std::ifstream reader_; // 输入流
    std::ofstream writer_; // 输出流

    int elementCount_; // 元素个数
    std::string backup_; //文件输出路径
    std::mutex mutex_; // 跳表锁
    std::string delimiter_; //分隔符
};

template <class Key, class Value>
list<Key, Value>::list(int maxLevel, std::string path, std::string delimiter)
    : maxLevel_(maxLevel),
      curLevel_(0),
      elementCount_(0),
      backup_(path),
      delimiter_(delimiter)
{
    Key key;
    Value value;
    header_ = new Node(key, value, maxLevel_);
}

template <class Key, class Value>
list<Key, Value>::~list()
{
    if(reader_.is_open())
        reader_.close();
    if(writer_.is_open())
        writer_.close();
    //只需要删除层级为0的节点
    if(header_->forward_[0] != nullptr)
        clear(header_->forward_[0]); //clear为递归删除
    delete header_;
}
//必须用离散概型 即掷硬币
template <class Key, class Value>
int list<Key, Value>::getRandomLevel()
{
    int counter = 0;
    // srand((unsigned)time(NULL));
    while(rand() % 2) counter ++; //如果随机数的最低为为1 则计数器增 一旦为0则不再增加
    counter = counter < maxLevel_ ? counter : maxLevel_;
    return counter;
}

template <class Key, class Value>
Node<Key, Value> *list<Key, Value>::createNode(Key key, Value value, int level)
{
    return new Node<Key, Value>(key, value, level);
}

template <class Key, class Value>
bool list<Key, Value>::insert(const Key& key, const Value& value)
{
    std::lock_guard<std::mutex> lock(mutex_);
    Node<Key, Value> *current = this->header_; // 遍历用的指针 初始为level0的头节点
    Node<Key, Value> *update[maxLevel_ + 1];    // 需要更新的节点的前驱
    std::fill(update, update + maxLevel_ + 1, nullptr);

    // 从最高层开始找 找插入节点的前驱 forward存的是节点在各个层的下一个节点！
    for (int i = curLevel_; i >= 0; i--)
    {
        // 在第i层上找最接近key的点
        while (current->forward_[i] != nullptr && current->forward_[i]->getKey() < key)
            current = current->forward_[i];
        update[i] = current; // update[i]就是插入节点在第i层的前驱
    }
    current = current->forward_[0]; // 此时current指向插入位置的后一个节点（可能为null）

    // 如果已经有节点了
    if (current != nullptr && current->getKey() == key)
    {
        //更新节点
        current->setValue(value);
        return true;
    }
    // 如果没有节点
    if (current == nullptr || current->getKey() != key)
    {
        int random = this->getRandomLevel(); // 获取随机层高
        //如果得到的随机层高比现在的层高更大 则需要增加现在的层高
        if (random > curLevel_)
        {
            // 高于当前层级的部分 前驱为header
            for (int i = curLevel_ + 1; i <= random; i++)
            {
                update[i] = header_;
            }
            curLevel_ = random;
        }

        Node<Key, Value> *node = createNode(key, value, random);
        // 插入节点则从0往高处遍历
        for (int i = 0; i <= random; i++)
        {
            node->forward_[i] = update[i]->forward_[i]; // 链接下一个节点
            update[i]->forward_[i] = node;              // 前驱指向当前节点
        }
        elementCount_++;
        return true;
    }
    return false;
}

template <class Key, class Value>
void list<Key, Value>::display() const
{
    std::cout << "\n*****list*****" << "\n";
    for (int i = 0; i <= curLevel_; i++)
    {
        Node<Key, Value>* node = this->header_->forward_[i];
        std::cout << "level " << i << " ";
        while(node != nullptr)
        {
            std::cout << node->getKey() << ": " << node->getValue() << " ";
            node = node->forward_[i];
        }
        std::cout << std::endl;
    }
}

template<class Key, class Value>
bool list<Key, Value>::find(const Key& key) const
{
    Node<Key, Value>* current = this->header_;
    for(int i = curLevel_;i >= 0; i--)
    {
        while(current->forward_[i] != nullptr && current->forward_[i]->getKey() < key)
            current = current->forward_[i];
    }
    current = current->forward_[0];
    if(current != nullptr && current->getKey() == key)
        return true;
    return false;
}

template<class Key, class Value>
bool list<Key, Value>::erase(const Key& key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    Node<Key, Value>* current = this->header_;
    Node<Key, Value>* update[maxLevel_ + 1];
    std::fill(update, update + maxLevel_ + 1, nullptr);

    for(int i = curLevel_;i >= 0;i --)
    {
        while(current->forward_[i] != nullptr && current->forward_[i]->getKey() < key)
            current = current->forward_[i];
        update[i] = current;
    }
    current = current->forward_[0];
    //找到了被删除节点
    if(current != nullptr && current->getKey() == key)
    {
        //从低到高删除该节点
        for(int i = 0;i <= curLevel_; i++)
        {
            if(update[i]->forward_[i] != current)
                break;
            update[i]->forward_[i] = current->forward_[i];
        }
        //删掉节点后可能导致高层没有节点了 删掉多余的层
        while(curLevel_ > 0 && header_->forward_[curLevel_] == nullptr) curLevel_--;

        //删除完毕
        delete current;
        elementCount_--;
        return true;
    }
    return false;
}

template<class Key, class Value>
void list<Key, Value>::clear(Node<Key, Value>* node)
{
    if(node->forward_[0] != nullptr)
        clear(node->forward_[0]);
    delete node;
}


template <class Key, class Value>
void list<Key, Value>::dump()
{
    writer_.open(backup_);
    Node<Key, Value>* node = this->header_->forward_[0];
    while(node != nullptr)
    {
        writer_ << node->getKey() << delimiter_ << node->getValue() << "\n";
        node = node->forward_[0];
    }
    writer_.flush();
    writer_.close();
    std::cout << "dump complete" << std::endl;
}

template<class Key, class Value>
bool list<Key, Value>::isValidString(const std::string &str)
{
    if(str.empty())
        return false;
    if(str.find(delimiter_) == std::string::npos)
        return false;
    return true;
}

template <class Key, class Value>
void list<Key, Value>::load()
{
    reader_.open(backup_, std::ios_base::in);
    if(!reader_.is_open()) return;
    std::string key_str;
    std::string value_str;
    std::string line;
    while(getline(reader_, line))
    {
        if(!isValidString(line))
            continue;
        auto delimiter = line.find(delimiter_);
        key_str = line.substr(0, delimiter);
        value_str = line.substr(delimiter+1, line.length()); //不包含\n 因为区间是左闭右开
        if(key_str.empty() || value_str.empty())
            continue;
        //通过istringstream 把泛型数值转化为Key和Value
        std::istringstream key_ss(key_str);
        std::istringstream value_ss(value_str);
        Key key;
        Value value;
        key_ss >> key;
        value_ss >> value;
        if(key_ss.fail() || value_ss.fail()) return;
        insert(key, value);
    }
    reader_.close();
}

template <class Key, class Value>
int list<Key, Value>::size() const
{
    return elementCount_;
}
