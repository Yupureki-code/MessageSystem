/**
 * @file test_friend.cpp
 * @brief 好友服务接口测试 (gtest版本)
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <chrono>

#include "comm_include/odb/friend/odb_friend.hpp"
#include "comm_include/odb/user/odb_user.hpp"
#include "comm_include/proto_include/friend.pb.h"
#include "comm_include/proto_include/user.pb.h"
#include "comm_include/proto_include/comm.pb.h"
#include "comm_include/logger.hpp"

using namespace messageSystem;

class FriendServiceTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        messageSystem::initLogger("test_friend_logger", "", spdlog::level::debug);

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

        _friend_db = std::make_shared<odbFriend::OdbFriend>(user, password, db, host, port);
        _user_db = std::make_shared<odbUser::OdbUser>(user, password, db, host, port);
    }

    void SetUp() override
    {
        // 创建测试用户（name限制20字符）
        std::string ts = std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count() % 100000);

        _user1 = std::make_shared<User>();
        _user1->name = "tf1_" + ts;
        _user1->password = "pass123";
        _user1->sex = Sex::male;
        _user1->age = 25;
        _user1->email = "f1_" + ts + "@test.com";
        _user1->desc = "test user 1";
        _user1->avatar = "";
        _user_db->insert(*_user1);

        _user2 = std::make_shared<User>();
        _user2->name = "tf2_" + ts;
        _user2->password = "pass456";
        _user2->sex = Sex::female;
        _user2->age = 23;
        _user2->email = "f2_" + ts + "@test.com";
        _user2->desc = "test user 2";
        _user2->avatar = "";
        _user_db->insert(*_user2);
    }

    void TearDown() override
    {
        // 清理好友关系
        std::shared_ptr<Friendships> fs;
        auto rep1 = _friend_db->selectByUid(_user1->uid, _user2->uid, &fs);
        if (rep1.status && fs)
            _friend_db->remove(*fs);
        auto rep2 = _friend_db->selectByUid(_user2->uid, _user1->uid, &fs);
        if (rep2.status && fs)
            _friend_db->remove(*fs);
    }

    static std::shared_ptr<odbFriend::OdbFriend> _friend_db;
    static std::shared_ptr<odbUser::OdbUser> _user_db;
    std::shared_ptr<User> _user1;
    std::shared_ptr<User> _user2;
};

std::shared_ptr<odbFriend::OdbFriend> FriendServiceTest::_friend_db;
std::shared_ptr<odbUser::OdbUser> FriendServiceTest::_user_db;

/// @brief 测试插入好友关系
TEST_F(FriendServiceTest, InsertFriendship)
{
    Friendships fs;
    fs.uid = _user1->uid;
    fs.friend_uid = _user2->uid;
    fs.status = FriendShipStatus::APPROVAL;
    fs.remark = "好友备注";
    fs.created_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    fs.updated_time = fs.created_time;

    auto rep = _friend_db->insert(fs);
    EXPECT_TRUE(rep.status) << rep.errmsg;
    EXPECT_GT(fs.id, 0);

    // 验证查询
    std::shared_ptr<Friendships> found;
    auto select_rep = _friend_db->selectByUid(_user1->uid, _user2->uid, &found);
    EXPECT_TRUE(select_rep.status) << select_rep.errmsg;
    EXPECT_TRUE(found != nullptr);
    EXPECT_EQ(found->uid, _user1->uid);
    EXPECT_EQ(found->friend_uid, _user2->uid);
    EXPECT_EQ(found->remark, "好友备注");
}

/// @brief 测试双向好友关系
TEST_F(FriendServiceTest, BidirectionalFriendship)
{
    // 创建双向关系
    unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    Friendships fs1;
    fs1.uid = _user1->uid;
    fs1.friend_uid = _user2->uid;
    fs1.status = FriendShipStatus::APPROVAL;
    fs1.remark = "user1对user2的备注";
    fs1.created_time = now;
    fs1.updated_time = now;
    auto rep1 = _friend_db->insert(fs1);
    EXPECT_TRUE(rep1.status) << rep1.errmsg;

    Friendships fs2;
    fs2.uid = _user2->uid;
    fs2.friend_uid = _user1->uid;
    fs2.status = FriendShipStatus::APPROVAL;
    fs2.remark = "user2对user1的备注";
    fs2.created_time = now;
    fs2.updated_time = now;
    auto rep2 = _friend_db->insert(fs2);
    EXPECT_TRUE(rep2.status) << rep2.errmsg;

    // 验证两个方向都能查到
    std::shared_ptr<Friendships> found1, found2;
    auto select_rep1 = _friend_db->selectByUid(_user1->uid, _user2->uid, &found1);
    auto select_rep2 = _friend_db->selectByUid(_user2->uid, _user1->uid, &found2);
    EXPECT_TRUE(select_rep1.status) << select_rep1.errmsg;
    EXPECT_TRUE(select_rep2.status) << select_rep2.errmsg;
    EXPECT_TRUE(found1 != nullptr);
    EXPECT_TRUE(found2 != nullptr);
    EXPECT_EQ(found1->remark, "user1对user2的备注");
    EXPECT_EQ(found2->remark, "user2对user1的备注");
}

/// @brief 测试更新好友备注
TEST_F(FriendServiceTest, UpdateRemark)
{
    // 创建好友关系
    Friendships fs;
    fs.uid = _user1->uid;
    fs.friend_uid = _user2->uid;
    fs.status = FriendShipStatus::APPROVAL;
    fs.remark = "原始备注";
    fs.created_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    fs.updated_time = fs.created_time;
    auto insert_rep = _friend_db->insert(fs);
    EXPECT_TRUE(insert_rep.status) << insert_rep.errmsg;

    // 更新备注
    std::shared_ptr<Friendships> found;
    auto select_rep = _friend_db->selectByUid(_user1->uid, _user2->uid, &found);
    EXPECT_TRUE(select_rep.status) << select_rep.errmsg;
    EXPECT_TRUE(found != nullptr);
    found->remark = "新备注";
    found->updated_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    auto update_rep = _friend_db->update(*found);
    EXPECT_TRUE(update_rep.status) << update_rep.errmsg;

    // 验证更新
    std::shared_ptr<Friendships> updated;
    auto select_rep2 = _friend_db->selectByUid(_user1->uid, _user2->uid, &updated);
    EXPECT_TRUE(select_rep2.status) << select_rep2.errmsg;
    EXPECT_TRUE(updated != nullptr);
    EXPECT_EQ(updated->remark, "新备注");
}

/// @brief 测试删除好友关系
TEST_F(FriendServiceTest, RemoveFriendship)
{
    // 创建好友关系
    Friendships fs;
    fs.uid = _user1->uid;
    fs.friend_uid = _user2->uid;
    fs.status = FriendShipStatus::APPROVAL;
    fs.remark = "";
    fs.created_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    fs.updated_time = fs.created_time;
    auto insert_rep = _friend_db->insert(fs);
    EXPECT_TRUE(insert_rep.status) << insert_rep.errmsg;

    // 删除
    std::shared_ptr<Friendships> found;
    auto select_rep = _friend_db->selectByUid(_user1->uid, _user2->uid, &found);
    EXPECT_TRUE(select_rep.status) << select_rep.errmsg;
    EXPECT_TRUE(found != nullptr);
    auto remove_rep = _friend_db->remove(*found);
    EXPECT_TRUE(remove_rep.status) << remove_rep.errmsg;

    // 验证删除
    std::shared_ptr<Friendships> deleted;
    auto select_rep2 = _friend_db->selectByUid(_user1->uid, _user2->uid, &deleted);
    EXPECT_TRUE(select_rep2.status) << select_rep2.errmsg;
    EXPECT_TRUE(deleted == nullptr);
}

/// @brief 测试用户查询
TEST_F(FriendServiceTest, SelectUserById)
{
    std::shared_ptr<User> found;
    auto rep = _user_db->selectById(std::to_string(_user1->uid), &found);
    EXPECT_TRUE(rep.status) << rep.errmsg;
    EXPECT_TRUE(found != nullptr);
    EXPECT_EQ(found->name, _user1->name);
    EXPECT_EQ(found->email, _user1->email);
}

/// @brief 测试按名称查找用户
TEST_F(FriendServiceTest, SelectUserByName)
{
    std::shared_ptr<User> found;
    auto rep = _user_db->selectByName(_user1->name, &found);
    EXPECT_TRUE(rep.status) << rep.errmsg;
    EXPECT_TRUE(found != nullptr);
    EXPECT_EQ(found->uid, _user1->uid);
}

/// @brief 测试CommRsp序列化（用于验证RPC响应）
TEST_F(FriendServiceTest, CommRspSerialization)
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

/// @brief 测试FindFriendByUIDRsp序列化
TEST_F(FriendServiceTest, FindFriendByUIDRspSerialization)
{
    FindFriendByUIDRsp rsp;
    rsp.mutable_response()->set_request_id("test_456");
    rsp.mutable_response()->set_status(true);
    rsp.mutable_friend_()->set_user_id("100");
    rsp.mutable_friend_()->set_nickname("test_user");
    rsp.mutable_friend_()->set_description("desc");
    rsp.mutable_friend_()->set_email("test@test.com");

    std::string bytes = rsp.SerializeAsString();
    FindFriendByUIDRsp rsp2;
    ASSERT_TRUE(rsp2.ParseFromString(bytes));
    EXPECT_EQ(rsp2.response().request_id(), "test_456");
    EXPECT_TRUE(rsp2.response().status());
    EXPECT_EQ(rsp2.friend_().user_id(), "100");
    EXPECT_EQ(rsp2.friend_().nickname(), "test_user");
}

/// @brief 测试FindFriendByNameRsp序列化
TEST_F(FriendServiceTest, FindFriendByNameRspSerialization)
{
    FindFriendByNameRsp rsp;
    rsp.mutable_response()->set_request_id("test_789");
    rsp.mutable_response()->set_status(true);
    auto* f1 = rsp.add_friends();
    f1->set_user_id("100");
    f1->set_nickname("user_a");
    auto* f2 = rsp.add_friends();
    f2->set_user_id("200");
    f2->set_nickname("user_b");

    std::string bytes = rsp.SerializeAsString();
    FindFriendByNameRsp rsp2;
    ASSERT_TRUE(rsp2.ParseFromString(bytes));
    EXPECT_EQ(rsp2.friends_size(), 2);
    EXPECT_EQ(rsp2.friends(0).nickname(), "user_a");
    EXPECT_EQ(rsp2.friends(1).nickname(), "user_b");
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
