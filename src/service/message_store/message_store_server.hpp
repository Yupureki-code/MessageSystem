#include "../../comm_include/proto_include/message_store.pb.h"
#include "../../comm_include/messageDB.hpp"
#include "../../comm_include/es.hpp"
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
            message.set_message_id(messages[i].message_id);
            message.set_conversation_id(messages[i].conversation_id);
            message.set_timestamp(boost::posix_time::to_time_t(messages[i].create_time));
            *message.mutable_sender() = users[i];
            auto content = message.mutable_message();
            switch(messages[i].message_type)
            {
                case MessageType::STRING:
                    content->set_message_type(STRING);
                    content->mutable_string_message()->set_content(messages[i].content.get());
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
        enum class TimestampPrecision 
        {
            Seconds,
            Milliseconds,
            Microseconds,
            Nanoseconds
        };
        std::string timestampToString(uint64_t timestamp, 
                                    TimestampPrecision precision = TimestampPrecision::Seconds) 
        {
            time_t time;
            switch (precision) {
                case TimestampPrecision::Seconds:
                    time = static_cast<time_t>(timestamp);
                    break;
                case TimestampPrecision::Milliseconds:
                    time = static_cast<time_t>(timestamp / 1000);
                    break;
                case TimestampPrecision::Microseconds:
                    time = static_cast<time_t>(timestamp / 1000000);
                    break;
                case TimestampPrecision::Nanoseconds:
                    time = static_cast<time_t>(timestamp / 1000000000);
                    break;
            }
            // ... 后续转换逻辑
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
            boost::posix_time::ptime stime = boost::posix_time::from_time_t(request->start_time());
            boost::posix_time::ptime etime = boost::posix_time::from_time_t(request->over_time());
            //2. 从数据库中进行消息查询
            std::vector<Message> messages;
            if(!_db->getHistoryMessages(cid, stime, etime, &messages))
            {
                LOG_ERROR("{} - 获取历史消息失败(cid):{}！", rid, cid);
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
            std::vector<std::string> uids;
            for(auto & it : messages)
            {
                if(it.message_type != 0)
                {
                    file_names.emplace_back(it.file_name.get());
                }
                uids.emplace_back(it.sender_id);
            }
            std::unordered_map<std::string, FileDownloadData> files;
            if(_db->getMessageFiles(rid, file_names, &files))
            {
                LOG_ERROR("{} - 获取历史文件失败(cid):{}！", rid, cid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            std::vector<UserInfo> users;
            if(!_db->getMessageUserInfo(rid, uids, &users))
            {
                LOG_ERROR("{} - 获取发送者信息失败(cid):{}！", rid, cid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            //5. 组织响应
            rep->set_status(true);
            rep->set_request_id(rid);
            for(int i = 0;i<messages.size();i++)
            {
                response->mutable_msg_list()->Add();
                *response->mutable_msg_list()->Mutable(i) = HandlerMessages(i, files, users, messages);
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
            if (!_db->getRecentMessages(cid, count, &messages)) 
            {
                LOG_ERROR("{} - 获取最近消息失败(cid):{}！", rid, cid);
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
            std::vector<std::string> uids;
            for(auto & it : messages)
            {
                if(it.message_type != 0)
                {
                    file_names.emplace_back(it.file_name.get());
                }
                uids.emplace_back(it.sender_id);
            }
            std::unordered_map<std::string, FileDownloadData> files;
            if(_db->getMessageFiles(rid, file_names, &files))
            {
                LOG_ERROR("{} - 获取历史文件失败(cid):{}！", rid, cid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            std::vector<UserInfo> users;
            if(!_db->getMessageUserInfo(rid, uids, &users))
            {
                LOG_ERROR("{} - 获取发送者信息失败(cid):{}！", rid, cid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            //5. 组织响应
            rep->set_status(true);
            rep->set_request_id(rid);
            for(int i = 0;i<messages.size();i++)
            {
                response->mutable_msg_list()->Add();
                *response->mutable_msg_list()->Mutable(i) = HandlerMessages(i, files, users, messages);
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
            {
            if(conds.has_start_time() && conds.has_end_time())
            {
                query.addFilterTimeRange("timestamp", timestampToString(conds.start_time()), timestampToString(conds.end_time()));
            }
            Json::Value value;
            if (!query.query("messageSystem", "_doc", &value)) 
            {
                LOG_ERROR("{} - 查询ES失败(cid):{}!", rid, cid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
        }
    private:
        std::shared_ptr<MessageDB> _db;
    };
}