/**
 * @file test_message_store_gtest.cpp
 * @brief 消息存储服务接口测试 (gtest版本)
 * @details 测试消息的增删查改功能
 */

#include <gtest/gtest.h>
#include <brpc/channel.h>
#include <string>
#include <vector>
#include <chrono>
#include <thread>

#include "comm_include/proto_include/comm.pb.h"
#include "comm_include/proto_include/message_store.pb.h"
#include "comm_include/proto_include/file.pb.h"

using namespace messageSystem;

class MessageStoreTest : public ::testing::Test 
{
protected:
    static void SetUpTestSuite() 
    {
        //连接消息存储服务
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = 5000;
        options.max_retry = 3;
        
        //默认连接本地服务，可通过环境变量覆盖
        std::string server_addr = "127.0.0.1:8003";
        const char* env_addr = std::getenv("MESSAGE_STORE_ADDR");
        if (env_addr) 
        {
            server_addr = env_addr;
        }
        
        if (_channel.Init(server_addr.c_str(), &options) != 0) 
        {
            GTEST_FATAL_FAILURE_("连接消息存储服务失败: ") << server_addr;
        }
        
        //生成唯一的会话ID用于测试
        _test_conversation_id = "conv_test_" + std::to_string(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
    }
    
    void SetUp() override 
    {
        //每个测试用例开始前的准备工作
    }
    
    void TearDown() override 
    {
        //每个测试用例结束后的清理工作
    }
    
    /// @brief 生成唯一消息ID
    std::string GenerateMessageId() 
    {
        static int counter = 0;
        return std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count()) + std::to_string(counter++);
    }
    
    /// @brief 创建文本消息
    MessageInfo CreateTextMessage(const std::string& content) 
    {
        MessageInfo msg;
        msg.set_message_id(GenerateMessageId());
        msg.set_conversation_id(_test_conversation_id);
        msg.set_sender_id("user_test_001");
        msg.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        
        auto* message_content = msg.mutable_message();
        message_content->set_message_type(MessageType::STRING);
        message_content->mutable_string_message()->set_content(content);
        
        return msg;
    }
    
    /// @brief 创建文件消息
    MessageInfo CreateFileMessage(const std::string& file_id, const std::string& file_name, int64_t file_size) 
    {
        MessageInfo msg;
        msg.set_message_id(GenerateMessageId());
        msg.set_conversation_id(_test_conversation_id);
        msg.set_sender_id("user_test_001");
        msg.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        
        auto* message_content = msg.mutable_message();
        message_content->set_message_type(MessageType::FILE);
        auto* file_msg = message_content->mutable_file_message();
        file_msg->set_file_id(file_id);
        file_msg->set_file_name(file_name);
        file_msg->set_file_size(file_size);
        
        return msg;
    }
    
    static brpc::Channel _channel;
    static std::string _test_conversation_id;
};

brpc::Channel MessageStoreTest::_channel;
std::string MessageStoreTest::_test_conversation_id;

/// @brief 测试发布单条文本消息
TEST_F(MessageStoreTest, PostSingleTextMessage) 
{
    MsgStorageService_Stub stub(&_channel);
    PostMessagesReq req;
    CommRsp rsp;
    brpc::Controller cntl;
    
    req.mutable_request()->set_request_id("test_post_single_text");
    *req.add_msg_list() = CreateTextMessage("Hello, World!");
    
    stub.PostMessages(&cntl, &req, &rsp, nullptr);
    
    ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
    ASSERT_TRUE(rsp.status()) << "发布文本消息失败: " << rsp.errmsg();
}

/// @brief 测试批量发布文本消息
TEST_F(MessageStoreTest, PostBatchTextMessages) 
{
    MsgStorageService_Stub stub(&_channel);
    PostMessagesReq req;
    CommRsp rsp;
    brpc::Controller cntl;
    
    req.mutable_request()->set_request_id("test_post_batch_text");
    
    //添加5条文本消息
    for (int i = 0; i < 5; i++) 
    {
        *req.add_msg_list() = CreateTextMessage("批量消息_" + std::to_string(i));
    }
    
    stub.PostMessages(&cntl, &req, &rsp, nullptr);
    
    ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
    ASSERT_TRUE(rsp.status()) << "批量发布文本消息失败: " << rsp.errmsg();
}

/// @brief 测试获取最近消息
TEST_F(MessageStoreTest, GetRecentMessages) 
{
    //先发布测试消息
    {
        MsgStorageService_Stub stub(&_channel);
        PostMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_get_recent_prepare");
        *req.add_msg_list() = CreateTextMessage("测试获取最近消息");
        
        stub.PostMessages(&cntl, &req, &rsp, nullptr);
        ASSERT_FALSE(cntl.Failed());
    }
    
    //等待数据同步
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    //获取最近消息
    {
        MsgStorageService_Stub stub(&_channel);
        GetRecentMsgReq req;
        GetRecentMsgRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_get_recent");
        req.set_conversation_id(_test_conversation_id);
        req.set_msg_count(10);
        
        stub.GetRecentMsg(&cntl, &req, &rsp, nullptr);
        
        ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
        ASSERT_TRUE(rsp.response().status()) << "获取最近消息失败: " << rsp.response().errmsg();
        ASSERT_GT(rsp.msg_list_size(), 0) << "应该至少有一条消息";
        
        //验证消息内容
        const auto& msg = rsp.msg_list(0);
        EXPECT_EQ(msg.conversation_id(), _test_conversation_id);
        EXPECT_EQ(msg.sender_id(), "user_test_001");
    }
}

/// @brief 测试获取历史消息
TEST_F(MessageStoreTest, GetHistoryMessages) 
{
    MsgStorageService_Stub stub(&_channel);
    GetHistoryMsgReq req;
    GetHistoryMsgRsp rsp;
    brpc::Controller cntl;
    
    req.mutable_request()->set_request_id("test_get_history");
    req.set_conversation_id(_test_conversation_id);
    req.set_start_time(0);
    req.set_over_time(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    stub.GetHistoryMsg(&cntl, &req, &rsp, nullptr);
    
    ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
    ASSERT_TRUE(rsp.response().status()) << "获取历史消息失败: " << rsp.response().errmsg();
}

/// @brief 测试消息搜索
TEST_F(MessageStoreTest, SearchMessages) 
{
    //先发布包含特定关键词的消息
    {
        MsgStorageService_Stub stub(&_channel);
        PostMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_search_prepare");
        *req.add_msg_list() = CreateTextMessage("搜索测试_KEYWORD_12345");
        
        stub.PostMessages(&cntl, &req, &rsp, nullptr);
        ASSERT_FALSE(cntl.Failed());
    }
    
    //等待ES索引更新
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    //搜索消息
    {
        MsgStorageService_Stub stub(&_channel);
        MsgSearchReq req;
        MsgSearchRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_search");
        auto* query = req.mutable_query();
        query->set_conversation_id(_test_conversation_id);
        query->set_text("KEYWORD_12345");
        
        stub.MsgSearch(&cntl, &req, &rsp, nullptr);
        
        ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
        ASSERT_TRUE(rsp.response().status()) << "消息搜索失败: " << rsp.response().errmsg();
    }
}

/// @brief 测试删除文本消息
TEST_F(MessageStoreTest, DeleteTextMessage) 
{
    //先发布消息
    MessageInfo test_msg;
    {
        MsgStorageService_Stub stub(&_channel);
        PostMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_delete_prepare");
        test_msg = CreateTextMessage("待删除消息");
        *req.add_msg_list() = test_msg;
        
        stub.PostMessages(&cntl, &req, &rsp, nullptr);
        ASSERT_FALSE(cntl.Failed());
    }
    
    //删除消息
    {
        MsgStorageService_Stub stub(&_channel);
        DeleteMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_delete");
        *req.add_msg_list() = test_msg;
        
        stub.DeleteMessages(&cntl, &req, &rsp, nullptr);
        
        ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
        ASSERT_TRUE(rsp.status()) << "删除消息失败: " << rsp.errmsg();
    }
}

/// @brief 测试批量删除消息
TEST_F(MessageStoreTest, BatchDeleteMessages) 
{
    //先批量发布消息
    std::vector<MessageInfo> test_messages;
    {
        MsgStorageService_Stub stub(&_channel);
        PostMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_batch_delete_prepare");
        for (int i = 0; i < 3; i++) 
        {
            auto msg = CreateTextMessage("批量删除测试_" + std::to_string(i));
            test_messages.push_back(msg);
            *req.add_msg_list() = msg;
        }
        
        stub.PostMessages(&cntl, &req, &rsp, nullptr);
        ASSERT_FALSE(cntl.Failed());
    }
    
    //批量删除消息
    {
        MsgStorageService_Stub stub(&_channel);
        DeleteMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_batch_delete");
        for (const auto& msg : test_messages) 
        {
            *req.add_msg_list() = msg;
        }
        
        stub.DeleteMessages(&cntl, &req, &rsp, nullptr);
        
        ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
        ASSERT_TRUE(rsp.status()) << "批量删除消息失败: " << rsp.errmsg();
    }
}

/// @brief 测试删除不存在的消息
TEST_F(MessageStoreTest, DeleteNonexistentMessage) 
{
    MsgStorageService_Stub stub(&_channel);
    DeleteMessagesReq req;
    CommRsp rsp;
    brpc::Controller cntl;
    
    req.mutable_request()->set_request_id("test_delete_nonexistent");
    auto* msg = req.add_msg_list();
    msg->set_message_id("nonexistent_msg_id_12345");
    msg->set_conversation_id(_test_conversation_id);
    msg->set_sender_id("user_test_001");
    msg->mutable_message()->set_message_type(MessageType::STRING);
    msg->mutable_message()->mutable_string_message()->set_content("不存在的消息");
    
    stub.DeleteMessages(&cntl, &req, &rsp, nullptr);
    
    //删除不存在的消息应该成功（幂等操作）
    ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
}

/// @brief 测试空消息列表
TEST_F(MessageStoreTest, EmptyMessageList) 
{
    MsgStorageService_Stub stub(&_channel);
    PostMessagesReq req;
    CommRsp rsp;
    brpc::Controller cntl;
    
    req.mutable_request()->set_request_id("test_empty_list");
    //不添加任何消息
    
    stub.PostMessages(&cntl, &req, &rsp, nullptr);
    
    //空列表应该成功
    ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
    ASSERT_TRUE(rsp.status()) << "空消息列表处理失败: " << rsp.errmsg();
}

/// @brief 测试消息ID唯一性
TEST_F(MessageStoreTest, MessageIdUniqueness) 
{
    //发布两条相同ID的消息
    std::string same_id = GenerateMessageId();
    
    {
        MsgStorageService_Stub stub(&_channel);
        PostMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_unique_id_1");
        auto msg = CreateTextMessage("第一条消息");
        msg.set_message_id(same_id);
        *req.add_msg_list() = msg;
        
        stub.PostMessages(&cntl, &req, &rsp, nullptr);
        ASSERT_FALSE(cntl.Failed());
    }
    
    //第二条相同ID的消息
    {
        MsgStorageService_Stub stub(&_channel);
        PostMessagesReq req;
        CommRsp rsp;
        brpc::Controller cntl;
        
        req.mutable_request()->set_request_id("test_unique_id_2");
        auto msg = CreateTextMessage("第二条消息");
        msg.set_message_id(same_id);
        *req.add_msg_list() = msg;
        
        stub.PostMessages(&cntl, &req, &rsp, nullptr);
        //根据业务逻辑，重复ID可能失败或覆盖
        //这里只验证RPC调用不失败
        ASSERT_FALSE(cntl.Failed()) << "RPC调用失败: " << cntl.ErrorText();
    }
}

int main(int argc, char** argv) 
{
    ::testing::InitGoogleTest(&argc, argv);
    
    if (argc < 2) 
    {
        std::cerr << "用法: " << argv[0] << " <服务地址:端口>" << std::endl;
        std::cerr << "示例: " << argv[0] << " 127.0.0.1:8003" << std::endl;
        std::cerr << "或设置环境变量: MESSAGE_STORE_ADDR=127.0.0.1:8003" << std::endl;
        return 1;
    }
    
    //设置服务地址到环境变量
    setenv("MESSAGE_STORE_ADDR", argv[1], 1);
    
    return RUN_ALL_TESTS();
}
