cmake_minmun_required(VERSION 3.10)
project(Raft)

find_package(protobuf REQUIRED)
find_package(gtest REQUIRED)
find_package(grpc CONFIG REQUIRED)

include_directories(./include)

add_subdirectory(./Common)
add_subdirectory(./Raft)
add_subdirectory(./Server)
add_subdirectory(./Test)
