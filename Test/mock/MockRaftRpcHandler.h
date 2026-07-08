#include <gmock/gmock.h>

#include "../../include/RaftRpcUtil.h"

class MockRaftRpcHandler: public RaftRpcHandler
{
    public:

        MOCK_METHOD(void, OnRequestVote, (const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply), (override));
        
        MOCK_METHOD(void, OnPreVote, (const RaftRpc::PreVoteArgs* request, RaftRpc::PreVoteReply *reply), (override));

        MOCK_METHOD(void, OnAppendEntriesStreamOn, (std::unique_ptr<AppendEntriesResponder> responder, const std::string& peer), (override));
        MOCK_METHOD(void, OnAppendEntries, (const RaftRpc::AppendEntriesArgs *request, const std::string& peer), (override));
        MOCK_METHOD(void, OnAppendEntriesStreamClose, (const std::string& peer), (override));

        MOCK_METHOD(void, OnInstallSnapshotStreamOn, (std::unique_ptr<InstallSnapshotResponder> responder, const std::string& peer), (override));
        MOCK_METHOD(void, OnInstallSnapshotChunk, (const RaftRpc::InstallSnapshotArgs *request, const std::string& peer), (override));
        MOCK_METHOD(void, OnInstallSnapshotStreamClose, (const std::string& peer), (override));
};