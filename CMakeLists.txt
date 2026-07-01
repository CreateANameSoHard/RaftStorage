cmake_minimum_required(VERSION 3.10)
project(Raft)

# 使用官方推荐的包名（首字母大写）
find_package(Protobuf REQUIRED)
find_package(GTest REQUIRED)      
find_package(gRPC CONFIG REQUIRED) 

# 全局头文件路径（可选）
include_directories(./include)

enable_testing()

# 假设你的 Common、Raft、Server 目录提供了库或源码
# 如果暂时不添加子目录，则测试中直接引用源文件（见下文）
# add_subdirectory(./Common)
# add_subdirectory(./Raft)
# add_subdirectory(./Server)
add_subdirectory(./Test)