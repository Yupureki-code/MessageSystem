#include "channel.hpp"
#include "comm.pb.h"
#include "comm.hpp"
#include "redis.hpp"
#include "odb/message/odb_message.hpp"
#include <elasticlient/client.h>
#include <memory>
#include "proto_include/file.pb.h"
#include "proto_include/user.pb.h"
#include "es.hpp"

namespace messageSystem
{
    class MessageDB
    {
    public:
        MessageDB(std::shared_ptr<ServiceManager> services)
        :_services(services)
        {
            
        }
        void InitODB(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        {
            _odb = std::make_unique<odbMessage::OdbMessage>(user,password,db,host,port);
        }
        void InitES(const std::string& host = "127.0.0.1:9200")
        {
            _client = std::make_shared<elasticlient::Client>(std::vector<std::string>{host});
        }
        bool getRecentMessages(const std::string& id,int count,std::vector<Message>* messages)
        {
            return _odb->getRecentMessages(id, count, messages);
        }
        bool getHistoryMessages(const std::string& id,boost::posix_time::ptime &start,boost::posix_time::ptime &end,std::vector<Message>* messages)
        {
            return _odb->getHistoryMessages(id, start, end, messages);
        }
        bool getMessageFiles(const std::string& rid,const std::vector<std::string>& file_names,std::unordered_map<std::string, FileDownloadData>* files)
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
            req.mutable_file_id_list()->Add(file_names.begin(), file_names.end());
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
        bool getMessageUserInfo(const std::string& rid,const std::vector<std::string>& uids,std::vector<UserInfo>* users)
        {
            ServiceChannel::ChannelPtr channel;
            if(!_services->chooseService(USER_SERVICE, &channel))
            {
                LOG_INFO("文件管理服务异常");
                return false;
            }
            messageSystem::UserService_Stub stub(channel.get());
            messageSystem::GetMultiUserInfoReq req;
            messageSystem::GetMultiUserInfoRsp rep;
            req.mutable_users_id()->Add(uids.begin(),uids.end());
            req.set_request_id(rid);
            brpc::Controller cntl;
            stub.GetMultiUserInfo(&cntl, &req, &rep, nullptr);
            if(cntl.Failed() || !rep.mutable_response()->status())
            {
                LOG_INFO("获取用户数据失败!");
                return false;
            }
            auto get_files = *rep.mutable_users_info();
            for(auto & it : get_files)
            {
                users->emplace_back(it.second);
            }
            return true;
        }
    private:
        std::unique_ptr<odbMessage::OdbMessage> _odb;
        std::shared_ptr<ServiceManager> _services;
        std::shared_ptr<elasticlient::Client> _client;
        ESQuery* _query;
        ESInsert* _insert;
    };
};