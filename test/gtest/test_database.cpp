/**
 * @file test_database_gtest.cpp
 * @brief 数据库操作测试 (gtest版本)
 * @details 测试ODB数据库操作
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <chrono>

#include "comm_include/odb/message/odb_message.hpp"
#include "comm_include/odb/message/odb_message_outbox.hpp"
#include "comm_include/logger.hpp"

using namespace odbMessage;

class DatabaseTest : public ::testing::Test 
{
protected:
    static void SetUpTestSuite() 
    {
        //初始化日志
        messageSystem::initLogger("test_logger", "", spdlog::level::debug);
        
        //从环境变量获取数据库配置
        std::string host = "127.0.0.1";
        std::string user = "test";
        std::string password = "test123";
        std::string db = "test";
        int port = 3306;
        
        const char* env_host = std::getenv("MYSQL_HOST");
        const char* env_user = std::getenv("MYSQL_USER");
        const char* env_password = std::getenv("MYSQL_PASSWORD");
        const char* env_db = std::getenv("MYSQL_DB");
        const char* env_port = std::getenv("MYSQL_PORT");
        
        if (env_host) host = env_host;
        if (env_user) user = env_user;
        if (env_password) password = env_password;
        if (env_db) db = env_db;
        if (env_port) port = std::stoi(env_port);
        
        _odb = std::make_unique<OdbMessage>(user, password, db, host, port);
        _outbox = std::make_unique<OdbMessageOutbox>(user, password, db, host, port);
        
        //生成测试会话ID
        _test_conversation_id = "conv_db_test_" + std::to_string(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
    }
    
    /// @brief 生成唯一消息ID
    std::string GenerateMessageId() 
    {
        static int counter = 0;
        return "db_msg_" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count()) + "_" + std::to_string(counter++);
    }
    
    /// @brief 创建测试文本消息
    Message CreateTextMessage(const std::string& content) 
    {
        Message msg;
        msg.message_id = GenerateMessageId();
        msg.conversation_id = _test_conversation_id;
        msg.sender_id = "user_db_test_001";
        msg.message_type = 0; //TEXT
        msg.create_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        msg.text = content;
        return msg;
    }
    
    static std::unique_ptr<OdbMessage> _odb;
    static std::unique_ptr<OdbMessageOutbox> _outbox;
    static std::string _test_conversation_id;
};

std::unique_ptr<OdbMessage> DatabaseTest::_odb;
std::unique_ptr<OdbMessageOutbox> DatabaseTest::_outbox;
std::string DatabaseTest::_test_conversation_id;

/// @brief 测试插入单条文本消息
TEST_F(DatabaseTest, InsertSingleTextMessage) 
{
    Message msg = CreateTextMessage("数据库测试消息");
    auto rep = _odb->insertMessage(msg);
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

/// @brief 测试批量插入文本消息
TEST_F(DatabaseTest, InsertBatchTextMessages) 
{
    std::vector<Message> messages;
    for (int i = 0; i < 5; i++) 
    {
        messages.push_back(CreateTextMessage("批量消息_" + std::to_string(i)));
    }
    auto rep = _odb->insertTextMessages(messages);
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

/// @brief 测试获取最近消息
TEST_F(DatabaseTest, GetRecentMessages) 
{
    //先插入测试消息
    std::vector<Message> messages;
    messages.push_back(CreateTextMessage("最近消息测试1"));
    messages.push_back(CreateTextMessage("最近消息测试2"));
    _odb->insertTextMessages(messages);
    
    //获取最近消息
    std::vector<Message> result;
    auto rep = _odb->getRecentMessages(_test_conversation_id, 10, &result);
    EXPECT_TRUE(rep.status) << rep.errmsg;
    EXPECT_GE(result.size(), 2);
}

/// @brief 测试获取历史消息
TEST_F(DatabaseTest, GetHistoryMessages) 
{
    unsigned long long start = 0;
    unsigned long long end = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    std::vector<Message> result;
    auto rep = _odb->getHistoryMessages(_test_conversation_id, start, end, &result);
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

/// @brief 测试删除消息
TEST_F(DatabaseTest, DeleteMessage) 
{
    //先插入测试消息
    Message msg = CreateTextMessage("待删除消息");
    _odb->insertMessage(msg);
    
    //删除消息
    auto rep = _odb->deleteMessage(msg.message_id);
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

/// @brief 测试删除会话所有消息
TEST_F(DatabaseTest, RemoveConversation) 
{
    //创建独立的测试会话
    std::string conv_id = "conv_remove_test_" + std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    
    Message msg;
    msg.message_id = GenerateMessageId();
    msg.conversation_id = conv_id;
    msg.sender_id = "user_test";
    msg.message_type = 0;
    msg.create_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    msg.text = "会话删除测试";
    
    _odb->insertMessage(msg);
    
    //删除整个会话
    auto rep = _odb->removeConversation(conv_id);
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

/// @brief 测试事务表插入
TEST_F(DatabaseTest, OutboxInsert) 
{
    MessageOutbox outbox;
    outbox.task_type = "INDEX_ES";
    outbox.conversation_id = _test_conversation_id;
    outbox.msg_id = 12345;
    outbox.payload = "{\"test\":\"data\"}";
    outbox.status = 0; //PENDING
    outbox.retry_count = 0;
    outbox.max_retries = 5;
    outbox.next_retry_at = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    outbox.created_at = outbox.next_retry_at;
    outbox.updated_at = outbox.next_retry_at;
    
    auto rep = _outbox->insert(outbox);
    EXPECT_TRUE(rep.status) << rep.errmsg;
    EXPECT_GT(outbox.id, 0);
}

/// @brief 测试事务表批量插入
TEST_F(DatabaseTest, OutboxBatchInsert) 
{
    std::vector<MessageOutbox> outboxes;
    unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (int i = 0; i < 3; i++) 
    {
        MessageOutbox outbox;
        outbox.task_type = "INDEX_ES";
        outbox.conversation_id = _test_conversation_id;
        outbox.msg_id = 100 + i;
        outbox.payload = "{\"test\":\"batch_" + std::to_string(i) + "\"}";
        outbox.status = 0;
        outbox.retry_count = 0;
        outbox.max_retries = 5;
        outbox.next_retry_at = now;
        outbox.created_at = now;
        outbox.updated_at = now;
        outboxes.push_back(outbox);
    }
    
    auto rep = _outbox->batchInsert(outboxes);
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

/// @brief 测试获取待处理任务
TEST_F(DatabaseTest, GetPendingTasks) 
{
    //先插入测试任务
    MessageOutbox outbox;
    outbox.task_type = "INDEX_ES";
    outbox.conversation_id = _test_conversation_id;
    outbox.msg_id = 99999;
    outbox.payload = "{\"test\":\"pending\"}";
    outbox.status = 0; //PENDING
    outbox.retry_count = 0;
    outbox.max_retries = 5;
    outbox.next_retry_at = 0; //立即可执行
    outbox.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    outbox.updated_at = outbox.created_at;
    _outbox->insert(outbox);
    
    //获取待处理任务
    auto tasks = _outbox->getPendingTasks(100);
    EXPECT_GE(tasks.size(), 1);
}

/// @brief 测试标记任务完成
TEST_F(DatabaseTest, MarkCompleted) 
{
    //先插入测试任务
    MessageOutbox outbox;
    outbox.task_type = "INDEX_ES";
    outbox.conversation_id = _test_conversation_id;
    outbox.msg_id = 88888;
    outbox.payload = "{\"test\":\"complete\"}";
    outbox.status = 0;
    outbox.retry_count = 0;
    outbox.max_retries = 5;
    outbox.next_retry_at = 0;
    outbox.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    outbox.updated_at = outbox.created_at;
    _outbox->insert(outbox);
    
    //标记完成
    auto rep = _outbox->markCompleted(outbox.id);
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

/// @brief 测试标记任务失败
TEST_F(DatabaseTest, MarkFailed) 
{
    //先插入测试任务
    MessageOutbox outbox;
    outbox.task_type = "INDEX_ES";
    outbox.conversation_id = _test_conversation_id;
    outbox.msg_id = 77777;
    outbox.payload = "{\"test\":\"fail\"}";
    outbox.status = 0;
    outbox.retry_count = 0;
    outbox.max_retries = 5;
    outbox.next_retry_at = 0;
    outbox.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    outbox.updated_at = outbox.created_at;
    _outbox->insert(outbox);
    
    //标记失败
    auto rep = _outbox->markFailed(outbox.id, "测试错误");
    EXPECT_TRUE(rep.status) << rep.errmsg;
}

int main(int argc, char** argv) 
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
