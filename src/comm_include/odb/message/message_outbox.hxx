#pragma once
#include <string>
#include <odb/core.hxx>
#include <odb/nullable.hxx>

enum class OutboxStatus : int {
    PENDING = 0,
    PROCESSING = 1,
    COMPLETED = 2,
    FAILED = 3
};

enum class OutboxTaskType : int {
    INDEX_ES = 0,
    DELETE_ES = 1,
    DELETE_FILE = 2
};

#pragma db object table("message_outbox")
struct MessageOutbox 
{
    #pragma db id auto
    unsigned long long id;
    
    #pragma db type("varchar(32)")
    std::string task_type;           //任务类型: INDEX_ES / DELETE_ES / DELETE_FILE
    
    #pragma db type("varchar(64)") index
    std::string conversation_id;     //关联的会话ID
    
    #pragma db index
    unsigned long long msg_id;       //关联的消息ID
    
    #pragma db type("TEXT")
    std::string payload;             //任务负载(JSON格式)
    
    int status;                      //状态: 0-PENDING, 1-PROCESSING, 2-COMPLETED, 3-FAILED
    int retry_count;                 //重试次数
    int max_retries;                 //最大重试次数
    unsigned long long next_retry_at;//下次重试时间(Unix毫秒)
    unsigned long long created_at;   //创建时间
    unsigned long long updated_at;   //更新时间
    
    #pragma db type("TEXT")
    odb::nullable<std::string> last_error; //最后一次错误信息
};

//odb -d mysql --std c++17 --generate-query --generate-schema message_outbox.hxx