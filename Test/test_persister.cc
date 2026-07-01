#include <gtest/gtest.h>
#include "../include/json.hpp"
#include "../include/Persister.h"

// test for basic file I/O
TEST(PersisterTest, PersisterInit)
{
    Persister persister(10);
    std::string state = "1";
    std::string snapshot = "2";
    persister.save(state, snapshot);
    EXPECT_EQ(persister.readRaftState(), "1");
};
// test for json read and write
TEST(PersisterTest, PersisterReadAndWrite)
{
    Persister persister(10);
    using nlohmann::json;
    json state_json =
        {
            {"currentTerm", 1},
            {"votedFor", 2},
            {"lastIncludeSnapshotIndex", 3},
            {"lastIncludeSnapshotTerm", 4},
            {"logs",
             {{{"logTerm", 1},
               {"logIndex", 2},
               {"command", 3}},
              {{"logTerm", 1},
               {"logIndex", 3},
               {"command", 4}}}}};
    json snapshot_json =
        {
            {{"logTerm", 1},
             {"logIndex", 2},
             {"command", 3}},
            {{"logTerm", 1},
             {"logIndex", 3},
             {"command", 4}}};
    // 上层传的的dump后的字符串 但实际持久化的是json
    std::string state = state_json.dump(4);
    std::string snapshot = snapshot_json.dump(4);
    persister.save(state, snapshot);

    EXPECT_EQ(state, persister.readRaftState());
    EXPECT_EQ(snapshot, persister.readSnapShot());
    EXPECT_EQ(5, persister.raftStateSize());
    EXPECT_EQ(2, persister.snapshotSize());
};

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}