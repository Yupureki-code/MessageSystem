cmake_minimum_required(VERSION 3.28.3)

project(MessageSystem LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---- 路径配置 ----
set(DATA_PATH ${CMAKE_SOURCE_DIR}/data)
set(LOG_PATH  ${CMAKE_SOURCE_DIR}/log)

set(COMM_INCLUDE_DIR   ${CMAKE_SOURCE_DIR}/src/comm_include)
set(PROTO_INCLUDE_DIR  ${CMAKE_SOURCE_DIR}/src/comm_include/proto_include)
set(SERVICE_DIR        ${CMAKE_SOURCE_DIR}/src/service)
set(OUTPUT_DIR         ${CMAKE_SOURCE_DIR}/output)

configure_file(${CMAKE_SOURCE_DIR}/config.in.h ${COMM_INCLUDE_DIR}/config.h)

# ---- 全局 include ----
include_directories(${COMM_INCLUDE_DIR})
include_directories(${PROTO_INCLUDE_DIR})
include_directories(${SERVICE_DIR})

# ---- 公共编译选项 ----
add_compile_options(-g -O2)

# ---- 额外的链接路径 ----
link_directories(/usr/local/lib)

# ---- 公共 Protobuf 源文件 ----
set(PROTO_COMMON_SOURCES
    ${PROTO_INCLUDE_DIR}/comm.pb.cc
    ${PROTO_INCLUDE_DIR}/file.pb.cc
    ${PROTO_INCLUDE_DIR}/message_store.pb.cc
    ${PROTO_INCLUDE_DIR}/user.pb.cc
    ${PROTO_INCLUDE_DIR}/conversation.pb.cc
    ${PROTO_INCLUDE_DIR}/friend.pb.cc
    ${PROTO_INCLUDE_DIR}/message.pb.cc
    ${PROTO_INCLUDE_DIR}/task_transfer.pb.cc
    ${PROTO_INCLUDE_DIR}/gateway.pb.cc
)

# ---- 公共 ODB 源文件 ----
set(ODB_USER_SOURCES
    ${COMM_INCLUDE_DIR}/odb/user/user-odb.cxx
)
set(ODB_FRIEND_SOURCES
    ${COMM_INCLUDE_DIR}/odb/friend/friend-odb.cxx
)
set(ODB_CONVERSATION_SOURCES
    ${COMM_INCLUDE_DIR}/odb/conversation/conversation-odb.cxx
)
set(ODB_MESSAGE_SOURCES
    ${COMM_INCLUDE_DIR}/odb/message/message-odb.cxx
    ${COMM_INCLUDE_DIR}/odb/message/message_outbox-odb.cxx
)

# ---- 公共链接库 ----
set(COMMON_LIBS
    pthread
    brpc
    protobuf
    gflags
    ssl
    crypto
    leveldb
    jsoncpp
    spdlog
    fmt
    etcd-cpp-api
    cpprest
    curl
)

set(DB_LIBS
    mysqlclient
    odb
    odb-mysql
)

set(ES_LIBS
    elasticlient
    cpr
)

set(MQ_LIBS
    amqpcpp
    ev
)
set(MQ_SOURCES
    /home/yupureki/software/AMQP-CPP/src/linux_tcp/tcpconnection.cpp
    /home/yupureki/software/AMQP-CPP/src/linux_tcp/openssl.cpp
    /home/yupureki/software/AMQP-CPP/src/linux_tcp/sslerrorprinter.cpp
)

set(REDIS_LIBS
    hiredis
    redis++
)

set(BOOST_LIBS
    boost_system
)

set(GATEWAY_LIBS
    ${BOOST_LIBS}
)

# ---- 添加子目录 ----
add_subdirectory(${SERVICE_DIR}/file)
add_subdirectory(${SERVICE_DIR}/user)
add_subdirectory(${SERVICE_DIR}/friend)
add_subdirectory(${SERVICE_DIR}/conversation)
add_subdirectory(${SERVICE_DIR}/message)
add_subdirectory(${SERVICE_DIR}/message_store)
add_subdirectory(${SERVICE_DIR}/task_transfer)
add_subdirectory(${SERVICE_DIR}/gateway)
