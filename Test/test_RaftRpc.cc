#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "../include/RaftRpcUtil.h"

class MockRaftRpcHandler: public RaftRpcHandler
{
    public:

        MOCK_METHOD(void, OnRequestVote, (const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply), (override));

        MOCK_METHOD(void, OnAppendEntriesStreamOn, (std::unique_ptr<AppendEntriesResponder> responder), (override));
        MOCK_METHOD(void, OnAppendEntries, (const RaftRpc::AppendEntriesArgs *request), (override));
        MOCK_METHOD(void, OnAppendEntriesStreamClose, (), (override));

        MOCK_METHOD(void, OnInstallSnapshotStreamOn, (std::unique_ptr<InstallSnapshotResponder> responder), (override));
        MOCK_METHOD(void, OnInstallSnapshotChunk, (const RaftRpc::InstallSnapshotArgs *request), (override));
        MOCK_METHOD(void, OnInstallSnapshotStreamClose, (), (override));
};

class RaftServerTest: public testing::Test
{
    protected:
        void SetUp() override
        {

        }
        void TearDown() override
        {

        }
};

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}