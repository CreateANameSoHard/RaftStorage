cmake_minimun_required(VERSION 3.10)
project(RaftTest)

find_package(protobuf REQUIRED)
find_package(grpc CONFIG REQUIRED)
find_package(gtest REQUIRED)
find_package(gmock REQUIRED)