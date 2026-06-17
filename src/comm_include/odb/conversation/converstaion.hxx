#pragma once
#include <string>
#include <odb/nullable.hxx>
#include <odb/core.hxx>
#include "../user/user.hxx"

enum class ConverstaionType
{
    PRIVATE = 0,
    GROUP = 1
};

enum class ConverstaionMemberPower
{
    COMMON = 0,
    ADMIN = 1,
    OWNER = 2
};

#pragma db object table("converstaion")
struct Converstaion
{
    #pragma db id auto
    size_t conversation_id;
    #pragma db type("varchar(30)")
    std::string name;
    ConverstaionType type;
    std::string avatar;
    unsigned long created_time;
    std::string last_message_content;
    std::string last_message_time;
};

#pragma db object table("converstaion_member")
struct ConverstaionMember
{
    #pragma db id auto
    size_t conversation_id;
    ConverstaionMemberPower power;
    size_t uid;
    std::string conversation_member_name;
    std::string conversation_remark_name;
};

#pragma db view object(Converstaion) \
                object(ConverstaionMember: ConverstaionMember::conversation_id == Converstaion::conversation_id) \
                object(User: User:uid != ConverstaionMember:uid)\
                query(Converstaion::type == "PRIVATE")
struct PrivateConversation
{
    #pragma db column(Converstaion::conversation_id)
    size_t conversation_id;
    #pragma db column(User::name)
    std::string name;
    #pragma db column(User::avatar)
    std::string avatar;
    #pragma db column(Converstaion::created_time)
    unsigned long created_time;
    #pragma db column(Converstaion::last_message_content)
    std::string last_message_content;
    #pragma db column(Converstaion::last_message_time)
    std::string last_message_time;
};