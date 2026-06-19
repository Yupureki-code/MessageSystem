#include "../../comm_include/proto_include/message_store.pb.h"
#include "../../comm_include/messageDB.hpp"
#include "../../comm_include/es.hpp"
#include <brpc/server.h>
#include <json/value.h>
#include <memory>
#include <unordered_map>
#include <vector>

namespace messageSystem
{
    class MessageServiceImpl : public messageSystem::MsgStorageService 
    {
    private:
        void HandlerError(CommRsp* rep,const std::string & rid,bool status,const std::string& msg)
        {
            rep->set_request_id(rid);
            rep->set_status(status);
            rep->set_errmsg(msg);
        }
        MessageInfo HandlerMessages(int i
            ,std::unordered_map<std::string, FileDownloadData>& files
            ,const std::vector<UserInfo>& users
            ,const std::vector<Message>& messages)
        {
            MessageInfo message;
            message.set_message_id(std::to_string(messages[i].message_id));
            message.set_conversation_id(messages[i].conversation_id);
            message.set_timestamp(messages[i].create_time);
            message.set_sender_id(messages[i].sender_id);
            auto content = message.mutable_message();
            switch(messages[i].message_type)
            {
                case MessageType::STRING:
                    content->set_message_type(STRING);
                    content->mutable_string_message()->set_content(messages[i].text.get());
                    break;
                case MessageType::IMAGE:
                    content->set_message_type(MessageType::IMAGE);
                    content->mutable_image_message()->set_file_id(messages[i].file_id.get());
                    content->mutable_image_message()->set_image_content(files[messages[i].file_id.get()].file_content());
                    break;
                case MessageType::FILE:
                    content->set_message_type(MessageType::FILE);
                    content->mutable_file_message()->set_file_id(messages[i].file_id.get());
                    content->mutable_file_message()->set_file_size(messages[i].file_size.get());
                    content->mutable_file_message()->set_file_name(messages[i].file_name.get());
                    content->mutable_file_message()->set_file_contents(files[messages[i].file_id.get()].file_content());
                    break;
                case MessageType::SPEECH:
                    content->mutable_file_message()->set_file_contents(files[messages[i].file_id.get()].file_content());
                    break;
                default:
                    break;
            }
            return message;
        }
    public:
        virtual void GetHistoryMsg(::google::protobuf::RpcController* controller,
            const ::messageSystem::GetHistoryMsgReq* request,
            ::messageSystem::GetHistoryMsgRsp* response,
            ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            //1. 提取关键要素：会话ID，起始时间，结束时间
            CommRsp* rep = response->mutable_response();
            std::string rid = request->request_id();
            std::string cid = request->conversation_id();
            unsigned long long stime = request->start_time();
            unsigned long long etime = request->over_time();
            //2. 从数据库中进行消息查询
            std::vector<Message> messages;
            auto hist_rep = _db->getHistoryMessages(cid, stime, etime, &messages);
            if(!hist_rep.status)
            {
                LOG_ERROR("{} - 获取历史消息失败(cid):{}:{}", rid, cid, hist_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            if (messages.empty()) 
            {
                rep->set_status(true);
                rep->set_request_id(rid);
                return;
            }
            //3. 统计所有文件类型消息的文件ID，并从文件子服务进行批量文件下载
            std::vector<std::string> file_names;
            for(auto & it : messages)
            {
                if(it.message_type != 0)
                {
                    file_names.emplace_back(it.file_name.get());
                }
            }
            std::unordered_map<std::string, FileDownloadData> files;
            if(!file_names.empty())
            {
                auto file_rep = _db->getMessageFiles(rid, file_names, &files);
                if(!file_rep.status)
                {
                    LOG_ERROR("{} - 获取历史文件失败(cid):{}:{}", rid, cid, file_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
            }
            //4. 组织响应
            rep->set_status(true);
            rep->set_request_id(rid);
            for(int i = 0;i<messages.size();i++)
            {
                response->mutable_msg_list()->Add();
                *response->mutable_msg_list()->Mutable(i) = HandlerMessages(i, files, {}, messages);
            }
            return;
        }
        virtual void GetRecentMsg(::google::protobuf::RpcController* controller,
            const ::messageSystem::GetRecentMsgReq* request,
            ::messageSystem::GetRecentMsgRsp* response,
            ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            //1. 提取请求中的关键要素：请求ID，会话ID，要获取的消息数量
            CommRsp* rep = response->mutable_response();
            std::string rid = request->request_id();
            int count = request->msg_count();
            std::string cid = request->conversation_id();
            //2. 从数据库，获取最近的消息元信息
            std::vector<Message> messages;
            auto recent_rep = _db->getRecentMessages(cid, count, &messages);
            if (!recent_rep.status) 
            {
                LOG_ERROR("{} - 获取最近消息失败(cid):{}:{}", rid, cid, recent_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
           if (messages.empty()) 
            {
                rep->set_status(true);
                rep->set_request_id(rid);
                return;
            }
            //3. 统计所有文件类型消息的文件ID，并从文件子服务进行批量文件下载
            std::vector<std::string> file_names;
            for(auto & it : messages)
            {
                if(it.message_type != 0)
                {
                    file_names.emplace_back(it.file_name.get());
                }
            }
            std::unordered_map<std::string, FileDownloadData> files;
            if(!file_names.empty())
            {
                auto file_rep = _db->getMessageFiles(rid, file_names, &files);
                if(!file_rep.status)
                {
                    LOG_ERROR("{} - 获取历史文件失败(cid):{}:{}", rid, cid, file_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
            }
            //4. 组织响应
            rep->set_status(true);
            rep->set_request_id(rid);
            for(int i = 0;i<messages.size();i++)
            {
                response->mutable_msg_list()->Add();
                *response->mutable_msg_list()->Mutable(i) = HandlerMessages(i, files, {}, messages);
            }
            return;
        }
        virtual void MsgSearch(::google::protobuf::RpcController* controller,
            const ::messageSystem::MsgSearchReq* request,
            ::messageSystem::MsgSearchRsp* response,
            ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            //关键字的消息搜索--只针对文本消息
            //1. 从请求中提取关键要素：请求ID，会话ID, 关键字
            CommRsp* rep = response->mutable_response();
            std::string rid = request->request_id();
            MessageQuery conds = request->query();
            std::string cid = conds.conversation_id();
            //2. 从ES搜索引擎中进行关键字消息搜索，得到消息列表
            ESQuery query;
            if(conds.has_text())
            {
                query.addMust("text", conds.text());
            }
            if(conds.has_sender_id())
            {
                query.addFilter("sender_id", {conds.sender_id()});
            }
            if(conds.has_start_time() && conds.has_end_time())
            {
                query.addFilterTimeRange("timestamp", conds.start_time(), conds.end_time());
            }
            Json::Value value;
            auto ret = query.query("messageSystem", "_doc", &value);
            if (!ret.status) 
            {
                LOG_ERROR("{} - {}:{}!", rid, ret.errmsg,cid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            
            if(value.isMember("hits") && value["hits"].isMember("hits") && value["hits"]["hits"].isArray())
            {
                auto array = value["hits"]["hits"];
                for(Json::Value::ArrayIndex i = 0;i<array.size();i++)
                {
                    MessageInfo message;
                    message.set_message_id(array[i]["message_id"].asString());
                    message.set_conversation_id(array[i]["conversation_id"].asString());
                    message.mutable_message()->set_message_type(MessageType::STRING);
                    message.mutable_message()->mutable_string_message()->set_content(array[i]["text"].asString());
                    response->mutable_msg_list()->Add();
                    *response->mutable_msg_list()->Mutable(i) = message;  
                }
            }
            else
            {
                LOG_ERROR("{} - 查询ES失败(cid):{}!", rid, cid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_status(true);
            rep->set_request_id(rid);
        }
        virtual void PostMessages(::google::protobuf::RpcController* controller,
            const ::messageSystem::PostMessagesReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            brpc::ClosureGuard rpc_guard(done);
            CommRsp* rep = response;
            std::string rid = request->request_id();
            
            //1. 分离文本消息和文件消息
            std::vector<Message> text_messages;
            std::vector<Message> file_messages;
            
            for(int i = 0; i < request->msg_list_size(); i++)
            {
                const auto& msg_info = request->msg_list(i);
                Message msg;
                msg.message_id = stoi(msg_info.message_id());
                msg.conversation_id = msg_info.conversation_id();
                msg.sender_id = msg_info.sender_id();
                msg.message_type = msg_info.message().message_type();
                msg.create_time = msg_info.timestamp();
                
                if(msg_info.message().message_type() == MessageType::STRING)
                {
                    msg.text = msg_info.message().string_message().content();
                    text_messages.push_back(msg);
                }
                else
                {
                    if(msg_info.message().has_file_message())
                    {
                        msg.file_id = msg_info.message().file_message().file_id();
                        msg.file_name = msg_info.message().file_message().file_name();
                        msg.file_size = msg_info.message().file_message().file_size();
                    }
                    else if(msg_info.message().has_image_message())
                    {
                        msg.file_id = msg_info.message().image_message().file_id();
                    }
                    else if(msg_info.message().has_speech_message())
                    {
                        msg.file_id = msg_info.message().speech_message().file_id();
                    }
                    file_messages.push_back(msg);
                }
            }
            
            //2. 处理文本消息
            if(!text_messages.empty())
            {
                auto text_rep = _db->PostTextMessages(text_messages);
                if(!text_rep.status)
                {
                    LOG_ERROR("{} - 发布文本消息失败:{}!", rid, text_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
            }
            
            //3. 处理文件消息
            if(!file_messages.empty())
            {
                auto file_rep = _db->PostFileMessages(rid, file_messages);
                if(!file_rep.status)
                {
                    LOG_ERROR("{} - 发布文件消息失败:{}!", rid, file_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
            }
            
            rep->set_status(true);
            rep->set_request_id(rid);
        }
        virtual void DeleteMessages(::google::protobuf::RpcController* controller,
            const ::messageSystem::DeleteMessagesReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            brpc::ClosureGuard rpc_guard(done);
            CommRsp* rep = response;
            std::string rid = request->request_id();
            
            //1. 分离文本消息和文件消息
            std::vector<Message> text_messages;
            std::vector<Message> file_messages;
            
            for(int i = 0; i < request->msg_list_size(); i++)
            {
                const auto& msg_info = request->msg_list(i);
                Message msg;
                msg.message_id = stoi(msg_info.message_id());
                msg.conversation_id = msg_info.conversation_id();
                msg.sender_id = msg_info.sender_id();
                msg.message_type = msg_info.message().message_type();
                msg.create_time = msg_info.timestamp();
                
                if(msg_info.message().message_type() == MessageType::STRING)
                {
                    msg.text = msg_info.message().string_message().content();
                    text_messages.push_back(msg);
                }
                else
                {
                    if(msg_info.message().has_file_message())
                    {
                        msg.file_id = msg_info.message().file_message().file_id();
                        msg.file_name = msg_info.message().file_message().file_name();
                        msg.file_size = msg_info.message().file_message().file_size();
                    }
                    else if(msg_info.message().has_image_message())
                    {
                        msg.file_id = msg_info.message().image_message().file_id();
                    }
                    else if(msg_info.message().has_speech_message())
                    {
                        msg.file_id = msg_info.message().speech_message().file_id();
                    }
                    file_messages.push_back(msg);
                }
            }
            
            //2. 处理文本消息删除
            if(!text_messages.empty())
            {
                auto del_text_rep = _db->DeleteTextMessages(text_messages);
                if(!del_text_rep.status)
                {
                    LOG_ERROR("{} - 删除文本消息失败:{}!", rid, del_text_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
            }
            
            //3. 处理文件消息删除
            if(!file_messages.empty())
            {
                auto del_file_rep = _db->DeleteFileMessages(rid, file_messages);
                if(!del_file_rep.status)
                {
                    LOG_ERROR("{} - 删除文件消息失败:{}!", rid, del_file_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
            }
            
            rep->set_status(true);
            rep->set_request_id(rid);
        }
    private:
        std::shared_ptr<MessageDB> _db;
    };
}