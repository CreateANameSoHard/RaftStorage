#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <thread>
#include <memory>
#include <iostream>

#include "../include/RaftRpcUtil.h"
#include "./mock/MockRaftRpcHandler.h"

class RaftRpcTest : public testing::Test
{
protected:
    void SetUp() override
    {
        handler_ = std::make_shared<MockRaftRpcHandler>();
        server_ = std::make_shared<RaftServer>("127.0.0.1", "8000", handler_.get());
        serverThread_ = std::make_shared<std::thread>(
            [&]() -> void
            {
                server_->run();
            });
        channel_ = grpc::CreateChannel("127.0.0.1:8000", grpc::InsecureChannelCredentials());
        client_ = std::make_shared<RaftClient>(channel_);
    }
    void TearDown() override
    {
        server_->shutdown();
        serverThread_->join();
    }
    std::shared_ptr<RaftServer> server_;
    std::shared_ptr<RaftClient> client_;
    std::shared_ptr<grpc::Channel> channel_;
    std::shared_ptr<MockRaftRpcHandler> handler_;
    std::shared_ptr<std::thread> serverThread_;
};

TEST_F(RaftRpcTest, RequestVoteTest)
{
    EXPECT_CALL(*handler_, OnRequestVote(testing::_, testing::_))
        .Times(1)
        .WillOnce(testing::Invoke(
            [](const RaftRpc::RequestVoteArgs *request, RaftRpc::RequestVoteReply *reply)
            {
                std::cout << "[Mock OnRequestVote]" << std::endl;
                reply->set_term(1);
                reply->set_votegranted(true);
                reply->set_votestate(1);
            }));

    /*
        message RequestVoteArgs
        {
            int32 Term=1; //当前任期 跟随者会在发送前自增任期
            int32 CandidateId=2; //当前候选者的id
            //这两项用于让系统拒绝给落后候选者投票
            int32 LastLogTerm=3;
            int32 LastLogIndex=4;
        }
        //投票响应
        message RequestVoteReply
        {
            int32 Term=1; //告诉候选者当前节点已知的系统最新任期 可以通过此数据让
            bool VoteGranted=2; //是否投给请求的候选者
            int32 VoteState=3; //当前节点状态
        }
    */
    grpc::ClientContext context;
    RaftRpc::RequestVoteArgs request;
    RaftRpc::RequestVoteReply reply;
    request.set_term(1);
    request.set_candidateid(1);
    request.set_lastlogindex(1);
    request.set_lastlogterm(1);
    client_->RequestVote(&context, request, &reply);

    EXPECT_EQ(reply.term(), 1);
    EXPECT_EQ(reply.votegranted(), true);
    EXPECT_EQ(reply.votestate(), 1);
};

TEST_F(RaftRpcTest, AppendEntriesTest)
{
    std::unique_ptr<AppendEntriesResponder> serverStream;
    {
        testing::Sequence seq;
        EXPECT_CALL(*handler_, OnAppendEntriesStreamOn(testing::_, testing::_))
            .Times(1)
            .WillOnce([&](std::unique_ptr<AppendEntriesResponder> responder, const std::string& peer)
                      {
                serverStream = std::move(responder);
                std::cout << "AppendEntries Stream got" << std::endl; });
        EXPECT_CALL(*handler_, OnAppendEntries(testing::_, testing::_))
            .Times(testing::AtLeast(1))
            .WillRepeatedly([&](const RaftRpc::AppendEntriesArgs *request, const std::string& peer)
                            {
                EXPECT_EQ(request->term(), 1);
                EXPECT_EQ(request->leaderid(), 2);
                EXPECT_EQ(request->prelogterm(), 1);
                EXPECT_EQ(request->prelogindex(), 10);

                RaftRpc::AppendEntriesReply reply;
                reply.set_term(1);
                reply.set_succss(true);
                reply.set_updatenextindex(10);
                reply.set_status(0);
                serverStream->SendReply(&reply); });
        EXPECT_CALL(*handler_, OnAppendEntriesStreamClose(testing::_))
            .Times(1)
            .WillOnce([&](const std::string& peer)
                      {
                serverStream->Close();
                EXPECT_TRUE(serverStream->Closed()); });
    }

    /*
        message AppendEntriesArgs
        {
            int32 Term=1; //消息的任期
            int32 LeaderId=2; //领导者id
            int32 PreLogIndex=3; //前一条日志的索引
            int32 PreLogTerm=4; //前一条日志的任期
            repeated LogEntry Entries=5; //发送的日志 如果为心跳消息，则为空
            int32 LeaderCommit=6; //领导者最新提交索引
        }
        //通信响应
        message AppendEntriesReply
        {
            int32 Term=1; //跟随者认为的当前领导者任期
            bool Succss=2; //跟随者是否接收成功
            int32 UpdateNextIndex=3; //根据跟随者的updateNextIndex值，领导者可以调整该跟随者的Next值
            int32 Status = 4; //跟随者状态
        }
    */

    grpc::ClientContext context;
    RaftRpc::AppendEntriesArgs request;
    RaftRpc::AppendEntriesReply reply;
    request.set_term(1);
    request.set_leaderid(2);
    request.set_prelogterm(1);
    request.set_prelogindex(10);
    for (int i = 0; i < 3; i++)
    {
        auto entry = request.add_entries();
        entry->set_logterm(1);
        entry->set_logindex(i + 1);
        entry->set_command("Set");
    }
    request.set_leadercommit(12);

    auto clientStream = client_->CreateAppendEntriesStream(&context);
    for (int i = 0; i < 10; i++)
    {
        bool success = clientStream->Write(&request);
        EXPECT_TRUE(success) << "stream write error while i = " << i;
    }
    for (int i = 0; i < 10; i++)
    {
        bool success = clientStream->Read(&reply);
        EXPECT_EQ(reply.term(), 1);
        EXPECT_TRUE(reply.succss());
        EXPECT_EQ(reply.updatenextindex(), 10);
        EXPECT_EQ(reply.status(), 0);

        reply.clear_term();
        reply.clear_succss();
        reply.clear_updatenextindex();
        reply.clear_status();
    }

    clientStream->Close();
    EXPECT_TRUE(clientStream->Closed());
};

TEST_F(RaftRpcTest, InstallSnapshotTest)
{
    std::unique_ptr<InstallSnapshotResponder> serverStream;
    {
        testing::InSequence seq;
        EXPECT_CALL(*handler_, OnInstallSnapshotStreamOn(testing::_, testing::_))
            .Times(1)
            .WillOnce(
                [&](std::unique_ptr<InstallSnapshotResponder> responder, const std::string& peer)
                {
                    serverStream = std::move(responder);
                    std::cout << "InstallSnapshot Stream got" << std::endl;
                });
        EXPECT_CALL(*handler_, OnInstallSnapshotChunk(testing::_, testing::_))
            .Times(testing::AtLeast(1))
            .WillRepeatedly(
                [&](const RaftRpc::InstallSnapshotArgs *request, const std::string& peer)
                {
                    EXPECT_EQ(request->leaderid(), 1);
                    EXPECT_EQ(request->term(), 10);
                    EXPECT_EQ(request->lastsnapshotincludeindex(), 20);
                    EXPECT_EQ(request->lastsnapshotincludeterm(), 10);
                    EXPECT_EQ(request->data(), "you are gay");
                    EXPECT_EQ(request->offset(), 0);

                    RaftRpc::InstallSnapshotReply reply;
                    reply.set_term(1);

                    serverStream->SendReply(&reply);
                });

        EXPECT_CALL(*handler_, OnInstallSnapshotStreamClose(testing::_))
            .Times(1)
            .WillOnce(
                [&](const std::string& peer)
                {
                    serverStream->Close();
                    EXPECT_TRUE(serverStream->Closed());
                });
    }

    grpc::ClientContext context;
    RaftRpc::InstallSnapshotArgs request;
    RaftRpc::InstallSnapshotReply reply;
    /*
        message InstallSnapshotArgs
        {
            int32 LeaderId=1;
            int32 Term=2;
            //最后被丢弃的日志索引 用于一致性检查
            int32 LastSnapShotIncludeIndex=3;
            int32 LastSnapShotIncludeTerm=4;
            bytes Data=5; //快照数据
            int32 Offset=6; //当前数据相较于起始快照的偏移量
        }

        message InstallSnapshotReply
        {
            int32 Term=1;
        }
    */
    request.set_leaderid(1);
    request.set_term(10);
    request.set_lastsnapshotincludeindex(20);
    request.set_lastsnapshotincludeterm(10);
    request.set_data("you are gay");
    request.set_offset(0);

    std::unique_ptr<InstallSnapshotStream> clientStream;
    clientStream = client_->CreateInstallSnapshotStream(&context);
    bool successWrite = clientStream->Write(&request);
    bool successRead = clientStream->Read(&reply);
    EXPECT_TRUE(successWrite);
    EXPECT_TRUE(successRead);
    EXPECT_EQ(reply.term(), 1);

    clientStream->Close();
    EXPECT_TRUE(clientStream->Closed());
};

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}