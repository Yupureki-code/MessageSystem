#pragma once
#include <string>
#include <odb/nullable.hxx>
#include <odb/core.hxx>
#include "../user/user.hxx"

enum class FriendShipStatus
{
    PENDING = 0,
    APPROVAL = 1,
    REJECT = 2
};

#pragma db object table("friendships")
struct Friendships
{
    #pragma db id auto
    unsigned long id;
    size_t uid;
    size_t friend_uid;
    FriendShipStatus status;
    #pragma db type("varchar(30)")
    std::string remark;
    unsigned long created_time;
    unsigned long updated_time;
    #pragma db index("uid_i") members(uid)
    #pragma db index("uid_friend_uid_i") members(uid,friend_uid)
};

#pragma db view object(Friendships) \
                object(User: User::uid == Friendships::friend_uid) \
                query((?))
struct FindFriend
{
    #pragma db column(Friendships::friend_uid)
    size_t friend_id;
    #pragma db column(User::name)
    std::string friend_name;
    #pragma db column(Friendships::remark)
    std::string remark;
    #pragma db column(User::age)
    int age;
    #pragma db column(User::email)
    std::string email;
    #pragma db column(User::desc)
    std::string desc;
    #pragma db column(User::avatar)
    std::string avatar;
    #pragma db column(User::sex)
    Sex sex;
    #pragma db column(Friendships::created_time)
    unsigned long created_time;
    #pragma db column(Friendships::updated_time)
    unsigned long updated_time;
};