#include <gtest/gtest.h>

#include <chrono>
#include <mutex>
#include <string>
#include <vector>

#include "comm.hpp"
#include "proto_include/conversation.pb.h"
#include "proto_include/message.pb.h"
#include "proto_include/message_store.pb.h"
#include "proto_include/task_transfer.pb.h"
#include "service/message/message_server.hpp"

namespace {

using namespace messageSystem;

class FakeConversationService : public ConversationServer {
public:
    std::vector<std::string> members;

    FakeConversationService() {
        members = {"u1", "u2", "u3"};
    }

    void CreateConversation(::google::protobuf::RpcController*,
        const ::messageSystem::CreateConversationReq*,
        ::messageSystem::CreateConversationRsp*,
        ::google::protobuf::Closure*) override {}

    void RemoveConversation(::google::protobuf::RpcController*,
        const ::messageSystem::RemoveConversationReq*,
        ::messageSystem::CommRsp*,
        ::google::protobuf::Closure*) override {}

    void AddMember(::google::protobuf::RpcController*,
        const ::messageSystem::AddMemberReq*,
        ::messageSystem::CommRsp*,
        ::google::protobuf::Closure*) override {}

    void ExitConversation(::google::protobuf::RpcController*,
        const ::messageSystem::ExitConversationReq*,
        ::messageSystem::CommRsp*,
        ::google::protobuf::Closure*) override {}

    void ChangeMemberPower(::google::protobuf::RpcController*,
        const ::messageSystem::ChangeMemberPowerReq*,
        ::messageSystem::CommRsp*,
        ::google::protobuf::Closure*) override {}

    void GetConversationMemberList(::google::protobuf::RpcController*,
        const ::messageSystem::GetConversationMemberListReq* request,
        ::messageSystem::GetConversationMemberListRsp* response,
        ::google::protobuf::Closure* done) override {
        if (done) {
            done->Run();
        }
        response->mutable_response()->set_request_id(request->request().request_id());
        response->mutable_response()->set_status(true);
        for (const auto& uid : members) {
            auto* user = response->add_user_infos();
            user->set_user_id(uid);
            user->set_nickname("nick_" + uid);
        }
    }
};

struct FakeRedis {
    std::mutex mu;
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> hash;

    Response HGet(const std::string& k, const std::string& f, std::string* out) {
        std::lock_guard<std::mutex> lock(mu);
        Response rep;
        auto kit = hash.find(k);
        if (kit == hash.end()) {
            rep.status = false;
            rep.errmsg = "not found";
            return rep;
        }
        auto fit = kit->second.find(f);
        if (fit == kit->second.end()) {
            rep.status = false;
            rep.errmsg = "not found";
            return rep;
        }
        *out = fit->second;
        rep.status = true;
        return rep;
    }

    Response HSet(const std::string& k, const std::string& f, const std::string& v) {
        std::lock_guard<std::mutex> lock(mu);
        Response rep;
        hash[k][f] = v;
        rep.status = true;
        return rep;
    }

    Response HIncrBy(const std::string& k, const std::string& f, int d) {
        std::lock_guard<std::mutex> lock(mu);
        Response rep;
        int cur = 0;
        auto kit = hash.find(k);
        if (kit != hash.end()) {
            auto fit = kit->second.find(f);
            if (fit != kit->second.end()) {
                cur = std::stoi(fit->second);
            }
        }
        cur += d;
        hash[k][f] = std::to_string(cur);
        rep.status = true;
        rep.value = cur;
        return rep;
    }
};

class MessageServerIntegrationTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        initLogger("message_server_test", "", spdlog::level::info);
    }

    void SetUp() override {
        redis.HSet("user:u1", "online", "true");
        redis.HSet("user:u2", "online", "false");
        redis.HSet("user:u3", "online", "true");
        redis.HSet("conversation:conv_1", "seq", "7");

        message_server.SetFetchConversationMembersForTest([this](const std::string&, const std::string&, std::vector<UserInfo>* user_infos) {
            user_infos->clear();
            for (const auto& uid : conversation_members) {
                UserInfo user;
                user.set_user_id(uid);
                user.set_nickname("nick_" + uid);
                user_infos->push_back(user);
            }
            return true;
        });
        message_server.SetRedisHGetForTest([this](const std::string& k, const std::string& f, std::string* out) {
            return redis.HGet(k, f, out);
        });
        message_server.SetRedisHSetForTest([this](const std::string& k, const std::string& f, const std::string& v) {
            return redis.HSet(k, f, v);
        });
        message_server.SetRedisHIncrByForTest([this](const std::string& k, const std::string& f, int d) {
            return redis.HIncrBy(k, f, d);
        });
        message_server.SetPublishForTest([this](const std::string& exchange, const std::string& body, const std::string& routing) {
            std::lock_guard<std::mutex> lock(pub_mu);
            published_exchange = exchange;
            published_routing = routing;
            published_body = body;
            publish_count++;
            return true;
        });
    }

    MessageInfo MakeTextMessage(const std::string& id) {
        MessageInfo msg;
        msg.set_message_id(id);
        msg.set_conversation_id("conv_1");
        msg.set_sender_id("u1");
        msg.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        auto* content = msg.mutable_message();
        content->set_message_type(MessageType::STRING);
        content->mutable_string_message()->set_content("hello");
        return msg;
    }

    FakeRedis redis;
    MessageServerImpl message_server;

    std::vector<std::string> conversation_members{"u1", "u2", "u3"};

    std::mutex pub_mu;
    std::string published_exchange;
    std::string published_routing;
    std::string published_body;
    int publish_count{0};
};

TEST_F(MessageServerIntegrationTest, SendMessageReturnsOnlineUsersAndPublishesPostTask) {
    SendMessageReq req;
    req.mutable_request()->set_request_id("rid_send");
    *req.mutable_message() = MakeTextMessage("m1");

    SendMessageRsq rsp;
    message_server.SendMessage(nullptr, &req, &rsp, nullptr);

    ASSERT_TRUE(rsp.response().status());
    ASSERT_EQ(rsp.uids_size(), 2);
    EXPECT_TRUE((rsp.uids(0) == "u1" || rsp.uids(1) == "u1"));
    EXPECT_TRUE((rsp.uids(0) == "u3" || rsp.uids(1) == "u3"));

    std::string unread;
    auto unread_rep = redis.HGet("user:u2", "unread:conv_1", &unread);
    ASSERT_TRUE(unread_rep.status);
    EXPECT_EQ(unread, "1");

    EXPECT_EQ(publish_count, 1);
    EXPECT_EQ(published_exchange, AMQP_MESSAGE_EXCHANGE);
    EXPECT_EQ(published_routing, AMQP_MESSAGE_POST_ROUTING_KEY);

    TaskPayload tp;
    ASSERT_TRUE(tp.ParseFromString(published_body));
    EXPECT_EQ(tp.routing_key(), AMQP_MESSAGE_POST_ROUTING_KEY);

    PostMessagesReq inner;
    ASSERT_TRUE(inner.ParseFromString(tp.payload_bytes()));
    ASSERT_EQ(inner.msg_list_size(), 1);
    EXPECT_EQ(inner.msg_list(0).message_id(), "m1");
}

TEST_F(MessageServerIntegrationTest, RecallMessageReturnsOnlineUsersIncrementsSeqAndPublishesDeleteTask) {
    RecallMessageReq req;
    req.mutable_request()->set_request_id("rid_recall");
    *req.mutable_message() = MakeTextMessage("m2");

    RecallMessageRsp rsp;
    message_server.RecallMessage(nullptr, &req, &rsp, nullptr);

    ASSERT_TRUE(rsp.response().status());
    ASSERT_EQ(rsp.uids_size(), 2);

    std::string seq;
    auto seq_rep = redis.HGet("conversation:conv_1", "seq", &seq);
    ASSERT_TRUE(seq_rep.status);
    EXPECT_EQ(seq, "8");

    EXPECT_EQ(publish_count, 1);
    EXPECT_EQ(published_exchange, AMQP_MESSAGE_EXCHANGE);
    EXPECT_EQ(published_routing, AMQP_MESSAGE_DELETE_ROUTING_KEY);

    TaskPayload tp;
    ASSERT_TRUE(tp.ParseFromString(published_body));
    EXPECT_EQ(tp.routing_key(), AMQP_MESSAGE_DELETE_ROUTING_KEY);

    DeleteMessagesReq inner;
    ASSERT_TRUE(inner.ParseFromString(tp.payload_bytes()));
    ASSERT_EQ(inner.msg_list_size(), 1);
    EXPECT_EQ(inner.msg_list(0).message_id(), "m2");
}

TEST_F(MessageServerIntegrationTest, MarkAsReadClearsUnreadCounter) {
    redis.HSet("user:u1", "unread:conv_1", "9");

    MarkAsReadReq req;
    req.mutable_request()->set_request_id("rid_read");
    req.set_coversation_id("conv_1");
    req.mutable_request()->set_uid("u1");

    CommRsp rsp;
    message_server.MarkAsRead(nullptr, &req, &rsp, nullptr);

    ASSERT_TRUE(rsp.status());

    std::string unread;
    auto unread_rep = redis.HGet("user:u1", "unread:conv_1", &unread);
    ASSERT_TRUE(unread_rep.status);
    EXPECT_EQ(unread, "0");
}

}  // namespace
