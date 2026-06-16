cmake_minimum_required(VERSION 3.28.3)
project(MessageStoreTest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

# 查找GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# 设置头文件路径
set(COMM_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../src/comm_include)
set(PROTO_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../src/comm_include/proto_include)
set(SERVICE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../src/service)

# 包含头文件目录
include_directories(${COMM_INCLUDE_DIR})
include_directories(${PROTO_INCLUDE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src)

# 查找源文件
set(PROTO_SOURCES 
    ${PROTO_INCLUDE_DIR}/file.pb.cc
    ${PROTO_INCLUDE_DIR}/message_store.pb.cc
    ${PROTO_INCLUDE_DIR}/user.pb.cc
    ${PROTO_INCLUDE_DIR}/task_transfer.pb.cc
    ${SERVICE_DIR}/comm.pb.cc
)
set(MESSAGE_ODB_SOURCES 
    ${COMM_INCLUDE_DIR}/odb/message/message-odb.cxx
    ${COMM_INCLUDE_DIR}/odb/message/message_outbox-odb.cxx
)

# 公共链接库
set(COMMON_LIBS
    pthread
    brpc
    protobuf
    gflags
    ssl
    crypto
    leveldb
    mysqlclient
    odb
    odb-mysql
    jsoncpp
    spdlog
    fmt
    elasticlient
    cpr
)

# 创建消息存储服务测试
add_executable(test_message_store 
    test_message_store.cpp
    ${PROTO_SOURCES}
    ${MESSAGE_ODB_SOURCES}
)
target_link_libraries(test_message_store ${GTEST_BOTH_LIBRARIES} ${COMMON_LIBS})

# 创建数据库操作测试
add_executable(test_database 
    test_database.cpp
    ${PROTO_SOURCES}
    ${MESSAGE_ODB_SOURCES}
)
target_link_libraries(test_database ${GTEST_BOTH_LIBRARIES} ${COMMON_LIBS})

# 创建任务中转逻辑测试
add_executable(test_task_transfer 
    test_task_transfer.cpp
    ${PROTO_SOURCES}
)
target_link_libraries(test_task_transfer ${GTEST_BOTH_LIBRARIES} ${COMMON_LIBS})

# 添加测试
add_test(NAME MessageStoreTest COMMAND test_message_store)
add_test(NAME DatabaseTest COMMAND test_database)
add_test(NAME TaskTransferTest COMMAND test_task_transfer)
