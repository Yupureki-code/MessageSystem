#pragma once
#include <cstdint>
#include <string>
#include <odb/nullable.hxx>
#include <odb/core.hxx>
#include "../user/user.hxx"

enum class ConversationType
{
    PRIVATE = 0,
    GROUP = 1
};

enum class ConversationMemberPower : int
{
    COMMON = 0,
    ADMIN = 1,
    OWNER = 2
};

#pragma db object table("Conversation")
struct Conversation
{
    #pragma db id auto
    size_t conversation_id;
    #pragma db type("varchar(30)")
    std::string name;
    ConversationType type;
    std::string avatar;
    size_t members;
    unsigned long created_time;
};

#pragma db object table("Conversation_member")
struct ConversationMember
{
    #pragma db id not_null
    uint64_t id;
    size_t conversation_id;
    ConversationMemberPower power;
    size_t uid;
    std::string conversation_member_name;
    std::string conversation_remark_name;
};

#pragma db view object(Conversation) \
                object(ConversationMember: ConversationMember::conversation_id == Conversation::conversation_id) \
                object(User: User::uid != ConversationMember::uid)\
                query(? == ConversationMember::uid)
struct PrivateConversation
{
    #pragma db column(Conversation::conversation_id)
    size_t conversation_id;
    #pragma db column(User::name)
    std::string name;
    #pragma db column(User::avatar)
    std::string avatar;
};


#pragma db view object(Conversation) \
                object(ConversationMember: ConversationMember::conversation_id == Conversation::conversation_id) \
                query(? == ConversationMember::uid)
struct GroupConversation
{
    #pragma db column(Conversation::conversation_id)
    size_t conversation_id;
    #pragma db column(ConversationMember::conversation_remark_name)
    std::string name;
    #pragma db column(Conversation::avatar)
    std::string avatar;
    #pragma db column(Conversation::members)
    size_t members;
};