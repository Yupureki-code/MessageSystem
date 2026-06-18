#pragma once
#include "../../comm_include/odb/friend/odb_friend.hpp"
#include "../../comm_include/odb/user/odb_user.hpp"
#include "../../comm_include/proto_include/friend.pb.h"
#include "../../comm_include/proto_include/user.pb.h"
#include "../../comm_include/proto_include/file.pb.h"
#include "../../comm_include/channel.hpp"
#include "../../comm_include/etcd.hpp"
#include <brpc/server.h>
#include <memory>

namespace messageSystem
{
    class FriendServiceImpl : public FriendServer
    {
    public:
        void InitODB(std::shared_ptr<odbFriend::OdbFriend> friend_db,
                     std::shared_ptr<odbUser::OdbUser> user_db)
        {
            _friend_db = friend_db;
            _user_db = user_db;
        }
        void InitService(std::shared_ptr<ServiceManager>& services)
        {
            _services = services;
        }
    private:
        void HandlerError(CommRsp* rep, const std::string& rid, bool status, const std::string& msg)
        {
            rep->set_request_id(rid);
            rep->set_status(status);
            rep->set_errmsg(msg);
        }
        /// @brief 将FindFriend视图填充到UserInfo
        void FillUserInfoFromView(UserInfo* info, const FindFriend& ff)
        {
            info->set_user_id(std::to_string(ff.friend_id));
            info->set_nickname(ff.friend_name);
            info->set_description(ff.desc);
            info->set_email(ff.email);
            info->set_avatar(ff.avatar);
        }
    public:
        /// @brief 发送好友请求
        virtual void FriendRequest(::google::protobuf::RpcController* controller,
            const ::messageSystem::FriendRequestReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done) override
        {
            LOG_DEBUG("收到好友请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request_id();
            CommRsp* rep = response;
            std::string uid = request->uid();
            std::string friend_uid = request->friend_uid();
            std::string remark = request->remark();

            // 1. 检查不能加自己为好友
            if (uid == friend_uid)
            {
                LOG_INFO("{} - 不能添加自己为好友！", rid);
                HandlerError(rep, rid, false, "不能添加自己为好友!");
                return;
            }
            // 2. 检查好友是否存在
            std::shared_ptr<User> friend_user;
            auto select_user_rep = _user_db->selectById(friend_uid, &friend_user);
            if (!select_user_rep.status || !friend_user)
            {
                LOG_INFO("{} - 好友用户不存在(uid):{}！", rid, friend_uid);
                HandlerError(rep, rid, false, "该用户不存在!");
                return;
            }
            // 3. 检查是否已经是好友
            std::shared_ptr<Friendships> existing;
            auto select_fs_rep = _friend_db->selectByUid(std::stoul(uid), std::stoul(friend_uid), &existing);
            if (!select_fs_rep.status)
            {
                LOG_ERROR("{} - 查询好友关系失败:{}！", rid, select_fs_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            if (existing)
            {
                LOG_INFO("{} - 已经是好友关系！", rid);
                HandlerError(rep, rid, false, "已经是好友关系!");
                return;
            }
            // 4. 创建好友关系（双向）
            unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            Friendships fs1;
            fs1.uid = std::stoul(uid);
            fs1.friend_uid = std::stoul(friend_uid);
            fs1.status = FriendShipStatus::APPROVAL;
            fs1.remark = remark;
            fs1.created_time = now;
            fs1.updated_time = now;
            auto insert_rep1 = _friend_db->insert(fs1);
            if (!insert_rep1.status)
            {
                LOG_ERROR("{} - 插入好友关系失败:{}！", rid, insert_rep1.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            // 反向关系
            Friendships fs2;
            fs2.uid = std::stoul(friend_uid);
            fs2.friend_uid = std::stoul(uid);
            fs2.status = FriendShipStatus::APPROVAL;
            fs2.remark = "";
            fs2.created_time = now;
            fs2.updated_time = now;
            auto insert_rep2 = _friend_db->insert(fs2);
            if (!insert_rep2.status)
            {
                LOG_ERROR("{} - 插入反向好友关系失败:{}！", rid, insert_rep2.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        /// @brief 修改好友备注
        virtual void FriendRemark(::google::protobuf::RpcController* controller,
            const ::messageSystem::FriendRemarkReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done) override
        {
            LOG_DEBUG("收到修改好友备注请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request_id();
            CommRsp* rep = response;
            std::string uid = request->uid();
            std::string friend_uid = request->friend_uid();
            std::string remark = request->remark();

            std::shared_ptr<Friendships> fs;
            auto select_fs_rep = _friend_db->selectByUid(std::stoul(uid), std::stoul(friend_uid), &fs);
            if (!select_fs_rep.status || !fs)
            {
                LOG_INFO("{} - 好友关系不存在！", rid);
                HandlerError(rep, rid, false, "好友关系不存在!");
                return;
            }
            fs->remark = remark;
            fs->updated_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            auto update_rep = _friend_db->update(*fs);
            if (!update_rep.status)
            {
                LOG_ERROR("{} - 更新好友备注失败:{}！", rid, update_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        /// @brief 通过UID查找好友
        virtual void FindFriendByUID(::google::protobuf::RpcController* controller,
            const ::messageSystem::FindFriendByUIDReq* request,
            ::messageSystem::FindFriendByUIDRsp* response,
            ::google::protobuf::Closure* done) override
        {
            LOG_DEBUG("收到通过UID查找好友请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request_id();
            CommRsp* rep = response->mutable_response();
            std::string uid = request->uid();
            std::string friend_uid = request->friend_uid();

            // 1. 检查好友关系是否存在
            std::shared_ptr<Friendships> fs;
            auto select_fs_rep = _friend_db->selectByUid(std::stoul(uid), std::stoul(friend_uid), &fs);
            if (!select_fs_rep.status || !fs)
            {
                LOG_INFO("{} - 好友关系不存在！", rid);
                HandlerError(rep, rid, false, "好友关系不存在!");
                return;
            }
            // 2. 获取好友用户信息
            std::shared_ptr<User> friend_user;
            auto select_user_rep = _user_db->selectById(friend_uid, &friend_user);
            if (!select_user_rep.status || !friend_user)
            {
                LOG_INFO("{} - 好友用户信息不存在(uid):{}！", rid, friend_uid);
                HandlerError(rep, rid, false, "好友信息不存在!");
                return;
            }
            // 3. 获取好友头像文件
            ServiceChannel::ChannelPtr channel;
            if (_services->chooseService(FILE_SERVICE, &channel))
            {
                messageSystem::FileService_Stub stub(channel.get());
                messageSystem::GetFileReq file_req;
                messageSystem::GetFileRsp file_rsp;
                file_req.set_request_id(rid);
                file_req.add_file_id_list(friend_user->avatar);
                brpc::Controller cntl;
                stub.GetMultiFile(&cntl, &file_req, &file_rsp, nullptr);
                if (!cntl.Failed() && file_rsp.success())
                {
                    auto* file_data = file_rsp.mutable_file_data();
                    if (file_data->count(friend_user->avatar))
                    {
                        response->mutable_friend_()->set_avatar((*file_data)[friend_user->avatar].file_content());
                    }
                }
            }
            // 使用FindFriend视图获取完整信息
            std::vector<FindFriend> friends;
            if (_friend_db->selectFriendsByName(std::stoul(uid), "", &friends))
            {
                for (const auto& ff : friends)
                {
                    if (ff.friend_id == std::stoul(friend_uid))
                    {
                        FillUserInfoFromView(response->mutable_friend_(), ff);
                        break;
                    }
                }
            }
            else
            {
                // fallback: 直接填充用户信息
                response->mutable_friend_()->set_user_id(friend_uid);
                response->mutable_friend_()->set_nickname(friend_user->name);
                response->mutable_friend_()->set_description(friend_user->desc);
                response->mutable_friend_()->set_email(friend_user->email);
                response->mutable_friend_()->set_avatar(friend_user->avatar);
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        /// @brief 通过名称搜索好友
        virtual void FindFriendByName(::google::protobuf::RpcController* controller,
            const ::messageSystem::FindFriendByNameReq* request,
            ::messageSystem::FindFriendByNameRsp* response,
            ::google::protobuf::Closure* done) override
        {
            LOG_DEBUG("收到通过名称搜索好友请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request_id();
            CommRsp* rep = response->mutable_response();
            std::string uid = request->uid();
            std::string name = request->name();

            // 1. 通过FindFriend视图直接查询好友
            std::vector<FindFriend> friends;
            auto select_rep = _friend_db->selectFriendsByName(std::stoul(uid), name, &friends);
            if (!select_rep.status)
            {
                LOG_ERROR("{} - 查询好友失败:{}！", rid, select_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            // 2. 批量获取头像
            std::vector<std::string> avatars;
            for (const auto& ff : friends)
            {
                if (!ff.avatar.empty())
                    avatars.push_back(ff.avatar);
            }
            std::unordered_map<std::string, FileDownloadData> file_map;
            if (!avatars.empty())
            {
                ServiceChannel::ChannelPtr channel;
                if (_services->chooseService(FILE_SERVICE, &channel))
                {
                    messageSystem::FileService_Stub stub(channel.get());
                    messageSystem::GetFileReq file_req;
                    messageSystem::GetFileRsp file_rsp;
                    file_req.set_request_id(rid);
                    for (const auto& a : avatars)
                    {
                        file_req.add_file_id_list(a);
                    }
                    brpc::Controller cntl;
                    stub.GetMultiFile(&cntl, &file_req, &file_rsp, nullptr);
                    if (!cntl.Failed() && file_rsp.success())
                    {
                        file_map = file_rsp.file_data();
                    }
                }
            }
            // 3. 组装响应
            for (const auto& ff : friends)
            {
                auto* info = response->add_friends();
                FillUserInfoFromView(info, ff);
                if (!ff.avatar.empty() && file_map.count(ff.avatar))
                {
                    info->set_avatar(file_map[ff.avatar].file_content());
                }
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
    private:
        std::shared_ptr<odbFriend::OdbFriend> _friend_db;
        std::shared_ptr<odbUser::OdbUser> _user_db;
        std::shared_ptr<ServiceManager> _services;
    };
    class FriendServerWrapper
    {
    private:
        void Put(const std::string& key, const std::string& value)
        {
            if (key == _file_service_base_url)
            {
                _services->addService(FILE_SERVICE, value);
                _services->activeService(key);
            }
        }
        void Del(const std::string& key, const std::string& value)
        {
            if (key == _file_service_base_url)
            {
                _services->inactiveService(key);
            }
        }
    public:
        FriendServerWrapper(const std::string& file_service_base_url, const std::string& host, const std::string& basedir)
            : _file_service_base_url(file_service_base_url)
            , _discover(host, basedir, std::bind(&FriendServerWrapper::Put, this, std::placeholders::_1, std::placeholders::_2)
                                   , std::bind(&FriendServerWrapper::Del, this, std::placeholders::_1, std::placeholders::_2))
        {
            _services = std::make_shared<ServiceManager>();
            _server.InitService(_services);
        }
        void InitODB(const std::string& user, const std::string& password,
                     const std::string& db, const std::string& host, int port)
        {
            auto friend_db = std::make_shared<odbFriend::OdbFriend>(user, password, db, host, port);
            auto user_db = std::make_shared<odbUser::OdbUser>(user, password, db, host, port);
            _server.InitODB(friend_db, user_db);
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
        std::string _file_service_base_url;
        Discovery _discover;
        FriendServiceImpl _server;
        std::shared_ptr<ServiceManager> _services;
    };
}
