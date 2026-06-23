#pragma once
#include <string>
#include <odb/nullable.hxx>
#include <odb/core.hxx>

enum class MessageType : int {
    TEXT = 0,
    IMAGE = 1,
    FILE = 2,
    VOICE = 3
};


#pragma db object table("message")
struct Message 
{
    #pragma db id
    unsigned long long message_id;
    unsigned long seq;
    #pragma db type("varchar(64)") index
    std::string conversation_id;                //所属会话ID
    #pragma db type("varchar(64)")
    std::string sender_id;                   //发送者用户ID
    int message_type;                       //消息类型 0-文本；1-图片；2-文件；3-语音
    #pragma db type("BIGINT UNSIGNED")
    unsigned long long create_time;        //消息的产生时间（毫秒级时间戳）

    odb::nullable<std::string> text;       //文本消息内容--非文本消息可以忽略
    #pragma db type("varchar(64)")
    odb::nullable<std::string> file_id;    //文件消息的文件ID -- 文本消息忽略
    #pragma db type("varchar(128)")
    odb::nullable<std::string> file_name;  //文件消息的文件名称 -- 只针对文件消息有效
    odb::nullable<unsigned int> file_size; //文件消息的文件大小 -- 只针对文件消息有效
};
//odb -d mysql --generate-query --generate-schema --std c++11 message.hxx