#pragma once
#include "conversationDB.hpp"
#include "../../comm_include/etcd.hpp"
#include "../../comm_include/channel.hpp"
#include "../../comm_include/proto_include/user.pb.h"
#include "../../comm_include/proto_include/file.pb.h"
#include <brpc/controller.h>
#include <brpc/server.h>
#include <memory>

namespace messageSystem
{
    class ConversationServerImpl : public ConversationServer
    {
    private:
        void HandlerError(CommRsp* response, const std::string& request_id, const std::string& errmsg)
        {
            response->set_status(false);
            response->set_errmsg(errmsg);
            response->set_request_id(request_id);
        }
        /// @brief 通过用户服务获取用户信息
        bool GetUserInfoFromService(const std::string& rid, const std::vector<std::string>& uids, 
                                   std::unordered_map<std::string, UserInfo>* user_map)
        {
            ServiceChannel::ChannelPtr channel;
            Response rep = _services->chooseService(USER_SERVICE, &channel);
            if (!rep.status)
            {
                LOG_INFO("{} - {}！", rid,rep.errmsg);
                return false;
            }
            messageSystem::UserService_Stub stub(channel.get());
            messageSystem::GetMultiUserInfoReq req;
            messageSystem::GetMultiUserInfoRsp rsp;
            req.mutable_request()->set_request_id(rid);
            for (const auto& uid : uids)
            {
                req.add_users_id(uid);
            }
            brpc::Controller cntl;
            stub.GetMultiUserInfo(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed() || !rsp.response().status())
            {
                LOG_INFO("{} - 获取用户信息失败！", rid);
                return false;
            }
            for(auto & it : *rsp.mutable_users_info())
            {
                user_map->insert({it.first,it.second});
            }
            return true;
        }
    public:
        void InitDB(std::shared_ptr<ConversationDB> db)
        {
            _db = db;
        }
        void InitService(std::shared_ptr<ServiceManager>& services)
        {
            _services = services;
        }
        void CreateConversation(google::protobuf::RpcController* controller,
                    const ::messageSystem::CreateConversationReq* request,
                    ::messageSystem::CreateConversationRsp* response,
                    ::google::protobuf::Closure* done) override
        {
            brpc::ClosureGuard rpc_guard(done);
            CommRsp* comm_rep = response->mutable_response();
            std::string rid = request->request().request_id();
            std::vector<ConversationMember> members;
            Conversation c;
            int size = request->comm_uid_size();
            unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            if (request->is_group())
            {
                members.resize(size + 1);
                members[size].id = now * 1000 + size;
                members[size].power = ConversationMemberPower::OWNER;
                members[size].uid = stoi(request->request().uid());
                c.name = request->group_conversation_name();
                c.type = ConversationType::GROUP;
            }
            else
            {
                members.reserve(size);
                c.type = ConversationType::PRIVATE;
            }
            for (int i = 0; i < size; i++)
            {
                members[i].id = now * 1000 + i;
                members[i].power = ConversationMemberPower::COMMON;
                members[i].uid = stoi(request->comm_uid(i));
            }
            size_t conv_id;
            Response rep = _db->insertConversation(c, &conv_id, members);
            if (!rep.status)
            {
                HandlerError(comm_rep, rid, "服务器繁忙,请稍后重试!");
                LOG_ERROR("{} - {}(id):{}", rid, rep.errmsg, conv_id);
                return;
            }
            comm_rep->set_status(true);
            comm_rep->set_request_id(rid);
            response->set_conversation_id(std::to_string(conv_id));
        }
        void RemoveConversation(google::protobuf::RpcController* controller,
                    const ::messageSystem::RemoveConversationReq* request,
                    ::messageSystem::CommRsp* response,
                    ::google::protobuf::Closure* done) override
        {
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string owner_id = request->request().uid();
            std::string cid = request->conversaion_id();
            auto rep = _db->removeConversation(cid, owner_id);
            if (!rep.status)
            {
                HandlerError(response, rid, rep.errmsg);
                LOG_ERROR("{} - {}(cid,uid):{},{}", rid, rep.errmsg, cid, owner_id);
                return;
            }
            response->set_status(true);
            response->set_request_id(rid);
        }
        void AddMember(google::protobuf::RpcController* controller,
                    const ::messageSystem::AddMemberReq* request,
                    ::messageSystem::CommRsp* response,
                    ::google::protobuf::Closure* done) override
        {
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string cid = request->conversaion_id();
            std::string uid = request->request().uid();

            ConversationMember member;
            member.id = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count() * 1000;
            member.conversation_id = std::stoi(cid);
            member.uid = std::stoi(uid);
            member.power = ConversationMemberPower::COMMON;

            auto rep = _db->addMember(member);
            if (!rep.status)
            {
                HandlerError(response, rid, rep.errmsg);
                LOG_ERROR("{} - 添加成员失败(cid,uid):{},{}:{}", rid, cid, uid, rep.errmsg);
                return;
            }
            response->set_status(true);
            response->set_request_id(rid);
        }
        void ExitConversation(google::protobuf::RpcController* controller,
                    const ::messageSystem::ExitConversationReq* request,
                    ::messageSystem::CommRsp* response,
                    ::google::protobuf::Closure* done) override
        {
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string cid = request->conversaion_id();
            std::string uid = request->request().uid();

            auto rep = _db->exitConversation(cid, uid);
            if (!rep.status)
            {
                HandlerError(response, rid, rep.errmsg);
                LOG_ERROR("{} - 退出会话失败(cid,uid):{},{}:{}", rid, cid, uid, rep.errmsg);
                return;
            }
            response->set_status(true);
            response->set_request_id(rid);
        }
        void ChangeMemberPower(google::protobuf::RpcController* controller,
                    const ::messageSystem::ChangeMemberPowerReq* request,
                    ::messageSystem::CommRsp* response,
                    ::google::protobuf::Closure* done) override
        {
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string cid = request->conversaion_id();
            std::string owner_id = request->request().uid();
            std::string uid = request->uid();
            int power = request->power();

            auto rep = _db->changeMemberPower(cid, uid, owner_id, power);
            if (!rep.status)
            {
                HandlerError(response, rid, rep.errmsg);
                LOG_ERROR("{} - 修改成员权限失败(cid,uid):{},{}:{}", rid, cid, uid, rep.errmsg);
                return;
            }
            response->set_status(true);
            response->set_request_id(rid);
        }
        void GetConversationMemberList(google::protobuf::RpcController* controller,
                    const ::messageSystem::GetConversationMemberListReq* request,
                    ::messageSystem::GetConversationMemberListRsp* response,
                    ::google::protobuf::Closure* done) override
        {
            brpc::ClosureGuard rpc_guard(done);
            CommRsp* comm_rep = response->mutable_response();
            std::string rid = request->request().request_id();
            std::string cid = request->conversaion_id();

            // 1. 从ES获取会话成员列表
            std::vector<UserInfo> users;
            auto rep = _db->getConversationMemberList(cid, &users);
            if (!rep.status)
            {
                HandlerError(comm_rep, rid, rep.errmsg);
                LOG_ERROR("{} - 获取会话成员列表失败(cid):{}:{}", rid, cid, rep.errmsg);
                return;
            }

            // 2. 收集所有用户ID
            std::vector<std::string> uids;
            for (const auto& user : users)
            {
                uids.push_back(user.user_id());
            }

            // 3. 从用户服务获取完整的用户信息
            if (!uids.empty())
            {
                std::unordered_map<std::string, UserInfo> user_map;
                if (GetUserInfoFromService(rid, uids, &user_map))
                {
                    // 4. 填充完整的用户信息
                    for (auto& user : users)
                    {
                        auto it = user_map.find(user.user_id());
                        if (it != user_map.end())
                        {
                            user = it->second;
                        }
                    }
                }
            }

            // 5. 填充响应
            comm_rep->set_status(true);
            comm_rep->set_request_id(rid);
            for (const auto& user : users)
            {
                *response->add_user_infos() = user;
            }
        }
        void SearchConversation(google::protobuf::RpcController* controller,
                    const ::messageSystem::SearchConversationReq* request,
                    ::messageSystem::SearchConversationRsp* response,
                    ::google::protobuf::Closure* done) override
        {
            brpc::ClosureGuard rpc_guard(done);
            CommRsp* comm_rep = response->mutable_response();
            std::string rid = request->request().request_id();
            std::string cid = request->conversaion_id();
            std::string cname = request->conversaion_name();
            std::vector<Conversation> find_conversations;
            Response rep = _db->SearchConversation(cid, cname, &find_conversations);
            if(!rep.status)
            {
                LOG_ERROR("{} - {}",rid,rep.errmsg);
                HandlerError(comm_rep, rid, rep.errmsg);
                return;
            }
            GetFileReq req;
            GetFileRsp rsp;
            req.mutable_request()->set_request_id(rid);
            for(const auto & it : find_conversations)
            {
                req.add_file_id_list(it.avatar);
            }
            ServiceChannel::ChannelPtr channel;
            rep = _services->chooseService(FILE_SERVICE, &channel);
            if(!rep.status)
            {
                LOG_ERROR("{} - {}",rid,rep.errmsg);
                HandlerError(comm_rep, rid, rep.errmsg);
                return;
            }
            FileService_Stub stub(channel.get());
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl, &req, &rsp, nullptr);
            for(auto & it : find_conversations)
            {
                ConversationInfo info;
                info.set_conversation_id(std::to_string(it.conversation_id));
                info.set_conversation_name(it.name);
                info.set_created_time(it.created_time);
                info.set_avatar(rsp.mutable_file_data()->find(it.avatar)->second.file_content());
            }
        }
    private:
        std::shared_ptr<ConversationDB> _db;
        std::shared_ptr<ServiceManager> _services;
    };
    class ConversationServerWrapper
    {
    private:
        void Put(const std::string& key, const std::string& value)
        {
            if (key == _user_service_base_url)
            {
                _services->addService(USER_SERVICE, value);
                _services->activeService(key);
            }
        }
        void Del(const std::string& key, const std::string& value)
        {
            if (key == _user_service_base_url)
            {
                _services->inactiveService(key);
            }
        }
    public:
        ConversationServerWrapper( const std::string& registry_host, const std::string& access_dir,const std::string& user_service_base_url = SERVICE_BASE_URL + USER_SERVICE)
            : _user_service_base_url(user_service_base_url)
            , _discover(registry_host, access_dir, std::bind(&ConversationServerWrapper::Put, this, std::placeholders::_1, std::placeholders::_2)
                                   , std::bind(&ConversationServerWrapper::Del, this, std::placeholders::_1, std::placeholders::_2))
        {
            _services = std::make_shared<ServiceManager>();
            _server.InitService(_services);
        }
        void InitDB(const std::string& user, const std::string& password,
                     const std::string& db, const std::string& host, int port)
        {
            auto db_ptr = std::make_shared<ConversationDB>(user, password, db, host, port);
            _server.InitDB(db_ptr);
        }
        void Start(uint16_t port, int32_t timeout, uint8_t num_threads)
        {
            _rpc_server = std::make_unique<brpc::Server>();
            int ret = _rpc_server->AddService(&_server, brpc::ServiceOwnership::SERVER_OWNS_SERVICE);
            if (ret == -1)
            {
                LOG_ERROR("添加RPC服务失败!");
                return;
            }
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout;
            options.num_threads = num_threads;
            ret = _rpc_server->Start(port, &options);
            if (ret == -1)
            {
                LOG_ERROR("服务启动失败!");
                return;
            }
        }
    private:
        std::unique_ptr<brpc::Server> _rpc_server;
        std::string _user_service_base_url;
        Discovery _discover;
        ConversationServerImpl _server;
        std::shared_ptr<ServiceManager> _services;
    };
}
