#pragma once
#include "channel.hpp"
#include "proto_include/comm.pb.h"
#include "comm.hpp"
#include "redis.hpp"
#include "odb/message/odb_message.hpp"
#include "odb/message/odb_message_outbox.hpp"
#include <elasticlient/client.h>
#include <json/value.h>
#include <json/writer.h>
#include <memory>
#include <sw/redis++/redis.h>
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
        void InitRedis(const std::string& ip = "127.0.0.1",int port = 6379,int thread_size = 10,int late_time = 5)
        {
            _redis = std::make_unique<redis::RedisClient>(ip,port,thread_size,late_time);
        }
        /// @brief 初始化ES连接
        void InitES(const std::string& host = "https://127.0.0.1:9200")
        {
            std::string es_url = host;
            const char* es_user = std::getenv("IM_ES_USER");
            const char* es_pass = std::getenv("IM_ES_PASS");
            if (es_user && es_pass && es_user[0] && es_pass[0])
            {
                // 插入认证信息: https://user:pass@host:port
                auto pos = es_url.find("://");
                if (pos != std::string::npos)
                {
                    es_url.insert(pos + 3, std::string(es_user) + ":" + es_pass + "@");
                }
            }
            // elasticlient要求URL以"/"结尾
            if (es_url.back() != '/') es_url += '/';
            _client = std::make_shared<elasticlient::Client>(std::vector<std::string>{es_url});
        }
        
        /// @brief 获取最近消息
        Response getRecentMessages(const std::string& id, int count, std::vector<Message>* messages)
        {
            return _odb->getRecentMessages(id, count, messages);
        }
        
        /// @brief 获取历史消息
        Response getHistoryMessages(const std::string& id, unsigned long long start, unsigned long long end, std::vector<Message>* messages)
        {
            return _odb->getHistoryMessages(id, start, end, messages);
        }
        
        /// @brief 获取消息文件
        Response getMessageFiles(const std::string& rid, const std::vector<std::string>& file_ids, std::unordered_map<std::string, FileDownloadData>* files)
        {
            Response rep;
            ServiceChannel::ChannelPtr channel;
            rep = _services->chooseService(FILE_SERVICE, &channel);
            if(!rep.status)
            {
                return rep;
            }
            messageSystem::FileService_Stub stub(channel.get());
            messageSystem::GetFileReq req;
            messageSystem::GetFileRsp file_rep;
            req.mutable_request()->set_request_id(rid);
            req.mutable_file_id_list()->Add(file_ids.begin(), file_ids.end());
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl, &req, &file_rep, nullptr);
            if(cntl.Failed() || !file_rep.success())
            {
                LOG_INFO("获取文件失败!");
                rep.status = false;
                rep.errmsg = cntl.Failed() ? cntl.ErrorText() : "获取文件失败";
                return rep;
            }
            auto get_files = *file_rep.mutable_file_data();
            for(auto & it : get_files)
            {
                files->insert(it);
            }
            return rep;
        }
        
        /// @brief 发布文本消息
        Response PostTextMessages(const std::vector<Message>& messages)
        {
            Response rep;
            //1. 写入MySQL
            auto odb_rep = _odb->insertTextMessages(messages);
            if(!odb_rep.status)
            {
                LOG_ERROR("写入MySQL文本消息失败:{}!", odb_rep.errmsg);
                return odb_rep;
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
                outbox.msg_id = msg.message_id;
                outbox.payload = buildTextMessageESDoc(msg);
                outbox.status = static_cast<int>(OutboxStatus::PENDING);
                outbox.retry_count = 0;
                outbox.max_retries = 5;
                outbox.next_retry_at = now;
                outbox.created_at = now;
                outbox.updated_at = now;
                outboxes.push_back(outbox);
            }
            
            auto outbox_rep = _outbox->batchInsert(outboxes);
            if(!outbox_rep.status)
            {
                LOG_ERROR("创建事务记录失败:{}!", outbox_rep.errmsg);
                //回滚MySQL
                for(const auto& msg : messages)
                {
                    _odb->deleteMessage(msg.message_id);
                }
                return outbox_rep;
            }
            
            //3. 尝试写入ES
            for(size_t i = 0; i < outboxes.size(); i++)
            {
                auto es_rep = insertESDoc(outboxes[i].payload);
                if(es_rep.status)
                {
                    _outbox->markCompleted(outboxes[i].id);
                }
                else
                {
                    _outbox->markFailed(outboxes[i].id, es_rep.errmsg);
                }
            }
            rep.status = true;
            return rep;
        }
        
        /// @brief 发布文件消息
        Response PostFileMessages(const std::string& rid, const std::vector<Message>& messages)
        {
            Response rep;
            //1. 写入MySQL
            auto odb_rep = _odb->insertFileMessages(messages);
            if(!odb_rep.status)
            {
                LOG_ERROR("写入MySQL文件消息失败:{}!", odb_rep.errmsg);
                return odb_rep;
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
                outbox.msg_id = msg.message_id;
                outbox.payload = buildFileMessageESDoc(msg);
                outbox.status = static_cast<int>(OutboxStatus::PENDING);
                outbox.retry_count = 0;
                outbox.max_retries = 5;
                outbox.next_retry_at = now;
                outbox.created_at = now;
                outbox.updated_at = now;
                outboxes.push_back(outbox);
            }
            
            auto outbox_rep = _outbox->batchInsert(outboxes);
            if(!outbox_rep.status)
            {
                LOG_ERROR("创建事务记录失败:{}!", outbox_rep.errmsg);
                //回滚MySQL
                for(const auto& msg : messages)
                {
                    _odb->deleteMessage(msg.message_id);
                }
                return outbox_rep;
            }
            
            //3. 尝试写入ES
            for(size_t i = 0; i < outboxes.size(); i++)
            {
                auto es_rep = insertESDoc(outboxes[i].payload);
                if(es_rep.status)
                {
                    _outbox->markCompleted(outboxes[i].id);
                }
                else
                {
                    _outbox->markFailed(outboxes[i].id, es_rep.errmsg);
                }
            }
            
            rep.status = true;
            return rep;
        }
        
        /// @brief 删除文本消息
        Response DeleteTextMessages(const std::vector<Message>& messages)
        {
            Response rep;
            //1. 从MySQL删除
            for(const auto& msg : messages)
            {
                auto odb_rep = _odb->deleteMessage(msg.message_id);
                if(!odb_rep.status)
                {
                    LOG_ERROR("从MySQL删除文本消息失败:{}!", odb_rep.errmsg);
                    return odb_rep;
                }
            }
            
            //2. 从ES删除
            for(const auto& msg : messages)
            {
                auto es_rep = deleteESDoc(std::to_string(msg.message_id));
                if(!es_rep.status)
                {
                    LOG_ERROR("从ES删除文本消息失败:{}!", es_rep.errmsg);
                    //创建重试任务
                    createDeleteESTask(msg);
                }
            }
            
            rep.status = true;
            return rep;
        }
        
        /// @brief 删除文件消息
        Response DeleteFileMessages(const std::string& rid, const std::vector<Message>& messages)
        {
            Response rep;
            //1. 从MySQL删除
            for(const auto& msg : messages)
            {
                auto odb_rep = _odb->deleteMessage(msg.message_id);
                if(!odb_rep.status)
                {
                    LOG_ERROR("从MySQL删除文件消息失败:{}!", odb_rep.errmsg);
                    return odb_rep;
                }
            }
            
            //2. 从ES删除
            for(const auto& msg : messages)
            {
                auto es_rep = deleteESDoc(std::to_string(msg.message_id));
                if(!es_rep.status)
                {
                    LOG_ERROR("从ES删除文件消息失败:{}!", es_rep.errmsg);
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
                auto file_rep = deleteFiles(rid, file_ids);
                if(!file_rep.status)
                {
                    LOG_ERROR("从文件服务删除文件失败:{}!", file_rep.errmsg);
                    //创建重试任务
                    for(const auto& file_id : file_ids)
                    {
                        createDeleteFileTask(file_id);
                    }
                }
            }
            
            rep.status = true;
            return rep;
        }
        
        /// @brief 处理待处理的事务任务
        void processPendingTasks()
        {
            auto tasks = _outbox->getPendingTasks(100);
            for(const auto& task : tasks)
            {
                _outbox->markProcessing(task.id);
                
                Response task_rep;
                if(task.task_type == "INDEX_ES")
                {
                    task_rep = insertESDoc(task.payload);
                }
                else if(task.task_type == "DELETE_ES")
                {
                    Json::Value value;
                    Json::Reader reader;
                    if(reader.parse(task.payload, value))
                    {
                        task_rep = deleteESDoc(value["message_id"].asString());
                    }
                    else
                    {
                        task_rep.status = false;
                        task_rep.errmsg = "JSON解析失败";
                    }
                }
                else if(task.task_type == "DELETE_FILE")
                {
                    Json::Value value;
                    Json::Reader reader;
                    if(reader.parse(task.payload, value))
                    {
                        std::vector<std::string> file_ids = {value["file_id"].asString()};
                        task_rep = deleteFiles(task.conversation_id, file_ids);
                    }
                    else
                    {
                        task_rep.status = false;
                        task_rep.errmsg = "JSON解析失败";
                    }
                }
                
                if(task_rep.status)
                {
                    _outbox->markCompleted(task.id);
                }
                else
                {
                    _outbox->markFailed(task.id, task_rep.errmsg);
                }
            }
        }
        /// @brief 获取ES客户端
        std::shared_ptr<elasticlient::Client> getESClient() const { return _client; }

    private:
        /// @brief 构建文本消息ES文档
        std::string buildTextMessageESDoc(const Message& msg)
        {
            Json::Value value;
            value["message_id"] = std::to_string(msg.message_id);
            value["conversation_id"] = msg.conversation_id;
            value["message_type"] = msg.message_type;
            value["sender_id"] = msg.sender_id;
            value["timestamp"] = std::to_string(msg.create_time);
            value["text"] = msg.text.get();
            return value.toStyledString();
        }
        
        /// @brief 构建文件消息ES文档
        std::string buildFileMessageESDoc(const Message& msg)
        {
            Json::Value value;
            value["message_id"] = std::to_string(msg.message_id);
            value["conversation_id"] = msg.conversation_id;
            value["message_type"] = msg.message_type;
            value["sender_id"] = msg.sender_id;
            value["timestamp"] = std::to_string(msg.create_time);
            value["file_id"] = msg.file_id.get();
            value["file_name"] = msg.file_name.get();
            value["file_size"] = msg.file_size.get();
            return value.toStyledString();
        }
        
        /// @brief 插入ES文档(ES不可用时跳过)
        Response insertESDoc(const std::string& doc)
        {
            Response rep;
            if (!_client) { rep.status = false; rep.errmsg = "ES client not initialized"; return rep; }
            try
            {
                ESInsert es(_client);
                Json::Value prefix;
                prefix["index"]["_index"] = "message";
                std::string prefix_str = prefix.toStyledString();
                std::string bulk_str = prefix_str + "\n" + doc + "\n";
                return es.batchInsert(bulk_str);
            }
            catch(const std::exception& e)
            {
                LOG_ERROR("插入ES文档失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 删除ES文档(ES不可用时跳过)
        Response deleteESDoc(const std::string& message_id)
        {
            Response rep;
            if (!_client) { rep.status = false; rep.errmsg = "ES client not initialized"; return rep; }
            try
            {
                ESInsert es(_client);
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
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 删除文件
        Response deleteFiles(const std::string& rid, const std::vector<std::string>& file_ids)
        {
            Response rep;
            ServiceChannel::ChannelPtr channel;
            rep = _services->chooseService(FILE_SERVICE, &channel);
            if(!rep.status)
            {
                return rep;
            }
            messageSystem::FileService_Stub stub(channel.get());
            messageSystem::DeleteFileReq req;
            messageSystem::CommRsp file_rep;
            req.mutable_request()->set_request_id(rid);
            req.mutable_file_id_list()->Add(file_ids.begin(), file_ids.end());
            brpc::Controller cntl;
            stub.DeleteMultiFile(&cntl, &req, &file_rep, nullptr);
            if(cntl.Failed() || !file_rep.status())
            {
                LOG_INFO("删除文件失败!");
                rep.status = false;
                rep.errmsg = cntl.Failed() ? cntl.ErrorText() : "删除文件失败";
                return rep;
            }
            rep.status = true;
            return rep;
        }
        
        /// @brief 创建删除ES任务
        void createDeleteESTask(const Message& msg)
        {
            unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            Json::Value payload;
            payload["message_id"] = std::to_string(msg.message_id);
            
            MessageOutbox outbox;
            outbox.task_type = "DELETE_ES";
            outbox.conversation_id = msg.conversation_id;
            outbox.msg_id = msg.message_id;
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
        std::unique_ptr<redis::RedisClient> _redis;
    };
}
