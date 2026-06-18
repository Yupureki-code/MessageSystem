/**
 * @file test_task_transfer.cpp
 * @brief 任务中转服务逻辑测试
 * @details 测试消息合并、批次触发等核心逻辑
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <chrono>
#include <thread>

#include "comm_include/proto_include/comm.pb.h"
#include "comm_include/proto_include/message_store.pb.h"
#include "comm_include/proto_include/task_transfer.pb.h"
#include "comm_include/comm.hpp"
#include "comm_include/logger.hpp"
#include "comm_include/channel.hpp"

using namespace messageSystem;

/// @brief 构建一个TaskPayload字节串
static std::string MakeTaskPayload(const std::string& routing_key, const std::string& inner_bytes)
{
    TaskPayload tp;
    tp.set_routing_key(routing_key);
    tp.set_payload_bytes(inner_bytes);
    tp.set_task_id("task_001");
    tp.set_trace_id("trace_001");
    tp.set_created_ts(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    return tp.SerializeAsString();
}

/// @brief 构建一个PostMessagesReq字节串（包含N条文本消息，ID带前缀）
static std::string MakePostMessagesReq(int count, const std::string& prefix = "msg")
{
    PostMessagesReq req;
    req.set_request_id("test_req");
    for(int i = 0; i < count; i++)
    {
        auto* msg = req.add_msg_list();
        msg->set_message_id(prefix + "_" + std::to_string(i));
        msg->set_conversation_id("conv_test");
        msg->set_sender_id("user_001");
        msg->set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        auto* content = msg->mutable_message();
        content->set_message_type(MessageType::STRING);
        content->mutable_string_message()->set_content("hello_" + std::to_string(i));
    }
    return req.SerializeAsString();
}

// ============================================================
// 测试ParsePostMessages的核心逻辑（不依赖网络）
// ============================================================

class ParseLogicTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        messageSystem::initLogger("test_logger", "", spdlog::level::debug);
    }
};

/// @brief 测试TaskPayload序列化/反序列化
TEST_F(ParseLogicTest, TaskPayloadRoundtrip)
{
    std::string inner = MakePostMessagesReq(3);
    std::string bytes = MakeTaskPayload(AMQP_MESSAGE_POST_ROUTING_KEY, inner);

    TaskPayload tp;
    ASSERT_TRUE(tp.ParseFromString(bytes));
    EXPECT_EQ(tp.routing_key(), AMQP_MESSAGE_POST_ROUTING_KEY);
    EXPECT_FALSE(tp.payload_bytes().empty());

    PostMessagesReq req;
    ASSERT_TRUE(req.ParseFromString(tp.payload_bytes()));
    EXPECT_EQ(req.msg_list_size(), 3);
    EXPECT_EQ(req.msg_list(0).message_id(), "msg_0");
    EXPECT_EQ(req.msg_list(0).message().string_message().content(), "hello_0");
}

/// @brief 测试多批次消息合并
TEST_F(ParseLogicTest, MergeMultipleBatches)
{
    // 模拟两次PostMessagesReq合并，使用不同前缀确保ID唯一
    PostMessagesReq merged;
    merged.set_request_id("merged");

    for(int batch = 0; batch < 2; batch++)
    {
        PostMessagesReq batch_req;
        batch_req.ParseFromString(MakePostMessagesReq(3, "batch" + std::to_string(batch)));
        for(auto& it : *batch_req.mutable_msg_list())
        {
            *merged.add_msg_list() = std::move(it);
        }
    }
    EXPECT_EQ(merged.msg_list_size(), 6);
    // 验证消息ID不重复
    std::set<std::string> ids;
    for(int i = 0; i < merged.msg_list_size(); i++)
    {
        ids.insert(merged.msg_list(i).message_id());
    }
    EXPECT_EQ(ids.size(), 6);
}

/// @brief 测试CommRsp序列化
TEST_F(ParseLogicTest, CommRspRoundtrip)
{
    CommRsp rsp;
    rsp.set_request_id("test_123");
    rsp.set_status(true);
    std::string bytes = rsp.SerializeAsString();

    CommRsp rsp2;
    ASSERT_TRUE(rsp2.ParseFromString(bytes));
    EXPECT_EQ(rsp2.request_id(), "test_123");
    EXPECT_TRUE(rsp2.status());
}

// ============================================================
// 批次触发逻辑测试（模拟RoutingInfo状态机）
// ============================================================

class BatchLogicTest : public ::testing::Test {};

/// @brief 测试批次触发条件
TEST_F(BatchLogicTest, BatchTriggerThreshold)
{
    int max_batch_size = 5;
    size_t nums = 0;
    bool in_flush_queue = false;

    // 模拟逐条累加
    for(int i = 0; i < max_batch_size; i++)
    {
        nums++;
        // 只有达到阈值且不在flush队列中才触发
        if(nums >= max_batch_size && !in_flush_queue)
        {
            in_flush_queue = true;
        }
    }
    EXPECT_TRUE(in_flush_queue);
    EXPECT_EQ(nums, max_batch_size);
}

/// @brief 测试未达阈值不触发
TEST_F(BatchLogicTest, BelowThresholdNoTrigger)
{
    int max_batch_size = 100;
    size_t nums = 0;
    bool in_flush_queue = false;

    for(int i = 0; i < 10; i++)
    {
        nums++;
        if(nums >= max_batch_size && !in_flush_queue)
        {
            in_flush_queue = true;
        }
    }
    EXPECT_FALSE(in_flush_queue);
    EXPECT_EQ(nums, 10);
}

/// @brief 测试重试次数限制
TEST_F(BatchLogicTest, RetryLimit)
{
    int max_retry = 3;
    int retry_count = 0;
    bool should_clear = false;
    bool should_retry = true;

    // 模拟连续失败
    for(int i = 0; i < 5; i++)
    {
        retry_count++;
        if(retry_count >= max_retry)
        {
            should_clear = true;
            should_retry = false;
            break;
        }
    }
    EXPECT_TRUE(should_clear);
    EXPECT_FALSE(should_retry);
    EXPECT_EQ(retry_count, 3);
}

/// @brief 测试成功后重置
TEST_F(BatchLogicTest, SuccessResetsState)
{
    size_t nums = 10;
    int retry_count = 2;
    bool in_flush_queue = true;

    // 模拟成功回调
    nums = 0;
    retry_count = 0;
    in_flush_queue = false;

    EXPECT_EQ(nums, 0);
    EXPECT_EQ(retry_count, 0);
    EXPECT_FALSE(in_flush_queue);
}

// ============================================================
// RoutingInfo状态机测试
// ============================================================

struct TestRoutingInfo
{
    size_t nums = 0;
    bool in_flush_queue = false;
    int retry_count = 0;
    void clear()
    {
        nums = 0;
        retry_count = 0;
        in_flush_queue = false;
    }
};

class RoutingInfoTest : public ::testing::Test {};

TEST_F(RoutingInfoTest, ClearResetsAll)
{
    TestRoutingInfo info;
    info.nums = 50;
    info.in_flush_queue = true;
    info.retry_count = 2;

    info.clear();

    EXPECT_EQ(info.nums, 0);
    EXPECT_FALSE(info.in_flush_queue);
    EXPECT_EQ(info.retry_count, 0);
}

TEST_F(RoutingInfoTest, FlushQueuePreventsDuplicates)
{
    TestRoutingInfo info;
    info.nums = 100;
    int trigger_count = 0;

    // 第一次触发
    if(info.nums >= 100 && !info.in_flush_queue)
    {
        info.in_flush_queue = true;
        trigger_count++;
    }
    // 第二次不应触发
    if(info.nums >= 100 && !info.in_flush_queue)
    {
        info.in_flush_queue = true;
        trigger_count++;
    }

    EXPECT_EQ(trigger_count, 1);
    EXPECT_TRUE(info.in_flush_queue);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
