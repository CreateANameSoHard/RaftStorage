cmake_minimum_required(VERSION 3.10)
project(RaftTest)

find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(GTest REQUIRED)

file(GLOB common ../Common/Util.cc ../Common/ThreadPool.cc)
file(GLOB raftrpc ../Raft/*.pb.cc ../Raft/RaftRpcUtil.cc)
file(GLOB raft ../Raft/*.cc ../Server/*.cc)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

include_directories(./mock)

# cmake -DCMAKE_BUILD_TYPE=Debug ..

add_executable(RaftRpcTest.out ./test_RaftRpc.cc ${common} ${raftrpc})
add_executable(RaftTest.out ./test_Raft.cc ${common} ${raft})
add_executable(KVServerTest.out ./test_KVServer.cc ${common} ${raft})

target_link_libraries(RaftRpcTest.out
    PRIVATE
    GTest::gtest_main
    GTest::gmock_main
    gRPC::grpc++
    protobuf
    pthread
)

target_link_libraries(RaftTest.out
    PRIVATE
    GTest::gtest_main
    gRPC::grpc++
    protobuf
    pthread
)

target_link_libraries(KVServerTest.out
    PRIVATE
    GTest::gtest_main
    gRPC::grpc++
    protobuf
    pthread
)

include(GoogleTest)
gtest_discover_tests(RaftRpcTest.out)
gtest_discover_tests(RaftTest.out)