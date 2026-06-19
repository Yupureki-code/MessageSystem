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
    ${PROTO_INCLUDE_DIR}/message.pb.cc
    ${PROTO_INCLUDE_DIR}/conversation.pb.cc
    ${PROTO_INCLUDE_DIR}/user.pb.cc
    ${PROTO_INCLUDE_DIR}/task_transfer.pb.cc
    ${PROTO_INCLUDE_DIR}/friend.pb.cc
    ${PROTO_INCLUDE_DIR}/comm.pb.cc
)
set(MESSAGE_ODB_SOURCES 
    ${COMM_INCLUDE_DIR}/odb/message/message-odb.cxx
    ${COMM_INCLUDE_DIR}/odb/message/message_outbox-odb.cxx
)
set(USER_ODB_SOURCES
    ${COMM_INCLUDE_DIR}/odb/user/user-odb.cxx
)
set(FRIEND_ODB_SOURCES
    ${COMM_INCLUDE_DIR}/odb/friend/friend-odb.cxx
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
    redis++
    amqpcpp
    ev
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

# 创建消息服务联调测试
add_executable(test_message_server
    test_message_server.cpp
    ${PROTO_SOURCES}
)
target_link_libraries(test_message_server ${GTEST_BOTH_LIBRARIES} ${COMMON_LIBS})

# 创建好友服务测试
add_executable(test_friend 
    test_friend.cpp
    ${PROTO_SOURCES}
    ${USER_ODB_SOURCES}
    ${FRIEND_ODB_SOURCES}
)
target_link_libraries(test_friend ${GTEST_BOTH_LIBRARIES} ${COMMON_LIBS})

# 添加测试
add_test(NAME MessageStoreTest COMMAND test_message_store)
add_test(NAME DatabaseTest COMMAND test_database)
add_test(NAME TaskTransferTest COMMAND test_task_transfer)
add_test(NAME MessageServerTest COMMAND test_message_server)
add_test(NAME FriendTest COMMAND test_friend)
