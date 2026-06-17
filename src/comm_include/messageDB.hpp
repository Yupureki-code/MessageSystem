#pragma once
#include "channel.hpp"
#include "comm.pb.h"
#include "comm.hpp"
#include "redis.hpp"
#include "odb/message/odb_message.hpp"
#include "odb/message/odb_message_outbox.hpp"
#include <elasticlient/client.h>
#include <json/value.h>
#include <json/writer.h>
#include <memory>
#include <vector>
#include "proto_include/file.pb.h"
#include "es.hpp"

namespace messageSystem
{
    /// @brief 消息数据库操作类
    class MessageDB
    {
    public:
        MessageDB(std::shared_ptr<ServiceManager> services)
        :_services(services)
        {
        }
        
        /// @brief 初始化ODB连接
        void InitODB(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        {
            _odb = std::make_unique<odbMessage::OdbMessage>(user,password,db,host,port);
            _outbox = std::make_unique<odbMessage::OdbMessageOutbox>(user,password,db,host,port);
        }
        
        /// @brief 初始化ES连接
        void InitES(const std::string& host = "127.0.0.1:9200")
        {
            _client = std::make_shared<elasticlient::Client>(std::vector<std::string>{host});
        }
        
        /// @brief 获取最近消息
        bool getRecentMessages(const std::string& id, int count, std::vector<Message>* messages)
        {
            return _odb->getRecentMessages(id, count, messages);
        }
        
        /// @brief 获取历史消息
        bool getHistoryMessages(const std::string& id, unsigned long long start, unsigned long long end, std::vector<Message>* messages)
        {
            return _odb->getHistoryMessages(id, start, end, messages);
        }
        
        /// @brief 获取消息文件
        bool getMessageFiles(const std::string& rid, const std::vector<std::string>& file_ids, std::unordered_map<std::string, FileDownloadData>* files)
        {
            ServiceChannel::ChannelPtr channel;
            if(!_services->chooseService(FILE_SERVICE, &channel))
            {
                LOG_INFO("文件管理服务异常");
                return false;
            }
            messageSystem::FileService_Stub stub(channel.get());
            messageSystem::GetFileReq req;
            messageSystem::GetFileRsp rep;
            req.mutable_file_id_list()->Add(file_ids.begin(), file_ids.end());
            req.set_request_id(rid);
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl, &req, &rep, nullptr);
            if(cntl.Failed() || !rep.success())
            {
                LOG_INFO("获取文件失败!");
                return false;
            }
            auto get_files = *rep.mutable_file_data();
            for(auto & it : get_files)
            {
                files->insert(it);
            }
            return true;
        }
        
        /// @brief 发布文本消息
        /// @param messages 消息列表
        /// @return 成功返回true
        bool PostTextMessages(const std::vector<Message>& messages)
        {
            //1. 写入MySQL
            if(!_odb->insertTextMessages(messages))
            {
                LOG_ERROR("写入MySQL文本消息失败!");
                return false;
            }
            
            //2. 创建事务记录
            std::vector<MessageOutbox> outboxes;
            unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            for(const auto& msg : messages)
            {
                MessageOutbox outbox;
                outbox.task_type = "INDEX_ES";
                outbox.conversation_id = msg.conversation_id;
                outbox.msg_id = std::stoull(msg.message_id);
                outbox.payload = buildTextMessageESDoc(msg);
                outbox.status = static_cast<int>(OutboxStatus::PENDING);
                outbox.retry_count = 0;
                outbox.max_retries = 5;
                outbox.next_retry_at = now;
                outbox.created_at = now;
                outbox.updated_at = now;
                outboxes.push_back(outbox);
            }
            
            if(!_outbox->batchInsert(outboxes))
            {
                LOG_ERROR("创建事务记录失败!");
                //回滚MySQL
                for(const auto& msg : messages)
                {
                    _odb->deleteMessage(msg.message_id);
                }
                return false;
            }
            
            //3. 尝试写入ES
            for(size_t i = 0; i < outboxes.size(); i++)
            {
                if(insertESDoc(outboxes[i].payload))
                {
                    _outbox->markCompleted(outboxes[i].id);
                }
                else
                {
                    _outbox->markFailed(outboxes[i].id, "ES写入失败");
                }
            }
            
            return true;
        }
        
        /// @brief 发布文件消息
        /// @param rid 请求ID
        /// @param messages 消息列表
        /// @return 成功返回true
        bool PostFileMessages(const std::string& rid, const std::vector<Message>& messages)
        {
            //1. 写入MySQL
            if(!_odb->insertFileMessages(messages))
            {
                LOG_ERROR("写入MySQL文件消息失败!");
                return false;
            }
            
            //2. 创建事务记录
            std::vector<MessageOutbox> outboxes;
            unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            for(const auto& msg : messages)
            {
                MessageOutbox outbox;
                outbox.task_type = "INDEX_ES";
                outbox.conversation_id = msg.conversation_id;
                outbox.msg_id = std::stoull(msg.message_id);
                outbox.payload = buildFileMessageESDoc(msg);
                outbox.status = static_cast<int>(OutboxStatus::PENDING);
                outbox.retry_count = 0;
                outbox.max_retries = 5;
                outbox.next_retry_at = now;
                outbox.created_at = now;
                outbox.updated_at = now;
                outboxes.push_back(outbox);
            }
            
            if(!_outbox->batchInsert(outboxes))
            {
                LOG_ERROR("创建事务记录失败!");
                //回滚MySQL
                for(const auto& msg : messages)
                {
                    _odb->deleteMessage(msg.message_id);
                }
                return false;
            }
            
            //3. 尝试写入ES
            for(size_t i = 0; i < outboxes.size(); i++)
            {
                if(insertESDoc(outboxes[i].payload))
                {
                    _outbox->markCompleted(outboxes[i].id);
                }
                else
                {
                    _outbox->markFailed(outboxes[i].id, "ES写入失败");
                }
            }
            
            return true;
        }
        
        /// @brief 删除文本消息
        /// @param messages 消息列表
        /// @return 成功返回true
        bool DeleteTextMessages(const std::vector<Message>& messages)
        {
            //1. 从MySQL删除
            for(const auto& msg : messages)
            {
                if(!_odb->deleteMessage(msg.message_id))
                {
                    LOG_ERROR("从MySQL删除文本消息失败:{}!", msg.message_id);
                    return false;
                }
            }
            
            //2. 从ES删除
            for(const auto& msg : messages)
            {
                if(!deleteESDoc(msg.message_id))
                {
                    LOG_ERROR("从ES删除文本消息失败:{}!", msg.message_id);
                    //创建重试任务
                    createDeleteESTask(msg);
                }
            }
            
            return true;
        }
        
        /// @brief 删除文件消息
        /// @param rid 请求ID
        /// @param messages 消息列表
        /// @return 成功返回true
        bool DeleteFileMessages(const std::string& rid, const std::vector<Message>& messages)
        {
            //1. 从MySQL删除
            for(const auto& msg : messages)
            {
                if(!_odb->deleteMessage(msg.message_id))
                {
                    LOG_ERROR("从MySQL删除文件消息失败:{}!", msg.message_id);
                    return false;
                }
            }
            
            //2. 从ES删除
            for(const auto& msg : messages)
            {
                if(!deleteESDoc(msg.message_id))
                {
                    LOG_ERROR("从ES删除文件消息失败:{}!", msg.message_id);
                    createDeleteESTask(msg);
                }
            }
            
            //3. 从文件服务删除文件
            std::vector<std::string> file_ids;
            for(const auto& msg : messages)
            {
                if(!msg.file_id.null())
                {
                    file_ids.push_back(msg.file_id.get());
                }
            }
            
            if(!file_ids.empty())
            {
                if(!deleteFiles(rid, file_ids))
                {
                    LOG_ERROR("从文件服务删除文件失败!");
                    //创建重试任务
                    for(const auto& file_id : file_ids)
                    {
                        createDeleteFileTask(file_id);
                    }
                }
            }
            
            return true;
        }
        
        /// @brief 处理待处理的事务任务
        void processPendingTasks()
        {
            auto tasks = _outbox->getPendingTasks(100);
            for(const auto& task : tasks)
            {
                _outbox->markProcessing(task.id);
                
                bool success = false;
                if(task.task_type == "INDEX_ES")
                {
                    success = insertESDoc(task.payload);
                }
                else if(task.task_type == "DELETE_ES")
                {
                    Json::Value value;
                    Json::Reader reader;
                    if(reader.parse(task.payload, value))
                    {
                        success = deleteESDoc(value["message_id"].asString());
                    }
                }
                else if(task.task_type == "DELETE_FILE")
                {
                    Json::Value value;
                    Json::Reader reader;
                    if(reader.parse(task.payload, value))
                    {
                        std::vector<std::string> file_ids = {value["file_id"].asString()};
                        success = deleteFiles(task.conversation_id, file_ids);
                    }
                }
                
                if(success)
                {
                    _outbox->markCompleted(task.id);
                }
                else
                {
                    _outbox->markFailed(task.id, "任务执行失败");
                }
            }
        }

    private:
        /// @brief 构建文本消息ES文档
        std::string buildTextMessageESDoc(const Message& msg)
        {
            Json::Value value;
            value["message_id"] = msg.message_id;
            value["conversation_id"] = msg.conversation_id;
            value["message_type"] = msg.message_type;
            value["sender_id"] = msg.sender_id;
            value["timestamp"] = msg.create_time;
            value["text"] = msg.text.get();
            return value.toStyledString();
        }
        
        /// @brief 构建文件消息ES文档
        std::string buildFileMessageESDoc(const Message& msg)
        {
            Json::Value value;
            value["message_id"] = msg.message_id;
            value["conversation_id"] = msg.conversation_id;
            value["message_type"] = msg.message_type;
            value["sender_id"] = msg.sender_id;
            value["timestamp"] = msg.create_time;
            value["file_id"] = msg.file_id.get();
            value["file_name"] = msg.file_name.get();
            value["file_size"] = msg.file_size.get();
            return value.toStyledString();
        }
        
        /// @brief 插入ES文档
        bool insertESDoc(const std::string& doc)
        {
            try
            {
                ESInsert es;
                Json::Value prefix;
                prefix["index"]["_index"] = "message";
                std::string prefix_str = prefix.toStyledString();
                std::string bulk_str = prefix_str + "\n" + doc + "\n";
                return es.batchInsert(bulk_str);
            }
            catch(const std::exception& e)
            {
                LOG_ERROR("插入ES文档失败:{}!", e.what());
                return false;
            }
        }
        
        /// @brief 删除ES文档
        bool deleteESDoc(const std::string& message_id)
        {
            try
            {
                ESInsert es;
                Json::Value prefix;
                prefix["delete"]["_index"] = "message";
                prefix["delete"]["_id"] = message_id;
                std::string prefix_str = prefix.toStyledString();
                std::string bulk_str = prefix_str + "\n";
                return es.batchInsert(bulk_str);
            }
            catch(const std::exception& e)
            {
                LOG_ERROR("删除ES文档失败:{}!", e.what());
                return false;
            }
        }
        
        /// @brief 删除文件
        bool deleteFiles(const std::string& rid, const std::vector<std::string>& file_ids)
        {
            ServiceChannel::ChannelPtr channel;
            if(!_services->chooseService(FILE_SERVICE, &channel))
            {
                LOG_INFO("文件管理服务异常");
                return false;
            }
            messageSystem::FileService_Stub stub(channel.get());
            messageSystem::DeleteFileReq req;
            messageSystem::CommRsp rep;
            req.mutable_file_id_list()->Add(file_ids.begin(), file_ids.end());
            req.set_request_id(rid);
            brpc::Controller cntl;
            stub.DeleteMultiFile(&cntl, &req, &rep, nullptr);
            if(cntl.Failed() || !rep.status())
            {
                LOG_INFO("删除文件失败!");
                return false;
            }
            return true;
        }
        
        /// @brief 创建删除ES任务
        void createDeleteESTask(const Message& msg)
        {
            unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            Json::Value payload;
            payload["message_id"] = msg.message_id;
            
            MessageOutbox outbox;
            outbox.task_type = "DELETE_ES";
            outbox.conversation_id = msg.conversation_id;
            outbox.msg_id = std::stoull(msg.message_id);
            outbox.payload = payload.toStyledString();
            outbox.status = static_cast<int>(OutboxStatus::PENDING);
            outbox.retry_count = 0;
            outbox.max_retries = 5;
            outbox.next_retry_at = now;
            outbox.created_at = now;
            outbox.updated_at = now;
            _outbox->insert(outbox);
        }
        
        /// @brief 创建删除文件任务
        void createDeleteFileTask(const std::string& file_id)
        {
            unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            Json::Value payload;
            payload["file_id"] = file_id;
            
            MessageOutbox outbox;
            outbox.task_type = "DELETE_FILE";
            outbox.conversation_id = "";
            outbox.msg_id = 0;
            outbox.payload = payload.toStyledString();
            outbox.status = static_cast<int>(OutboxStatus::PENDING);
            outbox.retry_count = 0;
            outbox.max_retries = 5;
            outbox.next_retry_at = now;
            outbox.created_at = now;
            outbox.updated_at = now;
            _outbox->insert(outbox);
        }

    private:
        std::unique_ptr<odbMessage::OdbMessage> _odb;
        std::unique_ptr<odbMessage::OdbMessageOutbox> _outbox;
        std::shared_ptr<ServiceManager> _services;
        std::shared_ptr<elasticlient::Client> _client;
    };
}
