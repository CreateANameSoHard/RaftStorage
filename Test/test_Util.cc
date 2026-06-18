#include <gtest/gtest.h>
#include <iostream>

#include "../include/Util.h"

TEST(UtilTest, ConcatTest)
{
    EXPECT_EQ(_CONCAT(1, 2), 12);
};

TEST(UtilTest, DeferTest)
{
    testing::internal::CaptureStdout();
    std::string result = "second print\nfirst print\n";

    {
        DEFER
        {
            std::cout << "first print" << std::endl;
        };
        DEFER
        {
            std::cout << "second print" << std::endl;
        };
    }
    std::string outputString = testing::internal::GetCapturedStdout();
    EXPECT_EQ(outputString, result);
};

TEST(UtilTest, ElectionTimeoutTest)
{
    for (int i = 0; i < 100; i++)
    {
        auto timeout = getRandomizedElectionTimeOut().count();
        EXPECT_LE(timeout, MaxRandomizedElectionTime);
        EXPECT_GE(timeout, MinRandomizedElectionTime);
    }
};

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}