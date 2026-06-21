#pragma once

#include <cctype>
#include <functional>
#include <memory>
#include <sw/redis++/utils.h>
#include <vector>
#include <odb/database.hxx>
#include <odb/transaction.hxx>
#include <odb/query.hxx>
#include <odb/result.hxx>
#include <odb/mysql/database.hxx>
#include "../../comm_include/odb/user/odb_user.hpp"
#include "../../comm_include/proto_include/user.pb.h"
#include <brpc/server.h>
#include "../../comm_include/es.hpp"
#include "../../comm_include/redis.hpp"
#include "../../comm_include/mail.hpp"
#include "../../comm_include/channel.hpp"
#include "../../comm_include/proto_include/file.pb.h"
#include "../../comm_include/etcd.hpp"

namespace messageSystem
{
    class UserServiceImpl : public UserService
    {
    private:
        bool checkPassword(const std::string& str)
        {
            if(str.size() < 6 || str.size() > 40)
                return false;
            for(const auto & it : str)
            {
                if(!std::isalnum(it) && it != '-' && it != '_')
                {
                    return false;
                }
            }
            return true;
        }
        bool checkName(const std::string& name)
        {
            return name.size() < 10;
        }
        void HandlerError(CommRsp* rep,const std::string & rid,bool status,const std::string& msg)
        {
            rep->set_request_id(rid);
            rep->set_status(status);
            rep->set_errmsg(msg);
        }
    public:
        void InitODB(std::shared_ptr<odbUser::OdbUser> odb)
        {
            _odb = odb;
        }
        void InitRedis(std::shared_ptr<redis::RedisClient> redis)
        {
            _redis = redis;
        }
        void InitService(std::shared_ptr<ServiceManager>& services)
        {
            _services = services;
        }
        virtual void UserLogin(::google::protobuf::RpcController* controller,
            const ::messageSystem::UserLoginReq* request,
            ::messageSystem::UserLoginRsp* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到用户登录请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request_id();
            std::string uid = request->uid();
            CommRsp* rep = response->mutable_response();
            std::string name = request->nickname();
            std::string password = request->password();
            std::shared_ptr<User> user;
            auto select_rep = _odb->selectByName(name, &user);
            if (!select_rep.status || !user || user->password != password)
            {
                LOG_INFO("{} - 用户名或密码错误(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "用户名或密码错误");
                return;
            }
            if (_redis->exists(uid))
            {
                LOG_INFO("{} - 用户已在其他地方登录(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "当前账号已在其他地方登陆");
                return;
            }
            std::string ssid = util::StringUtil::generateUniqueName();
            auto redis_rep = _redis->setex(uid, ssid);
            if (!redis_rep.status)
            {
                LOG_ERROR("{} - Redis设置失败:{}！", rid, redis_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            std::string token = util::StringUtil::generateUniqueName();
            redis_rep = _redis->setex("token:uid", token,3600);
            if(!redis_rep.status)
            {
                LOG_ERROR("{} - Redis设置失败:{}！", rid, redis_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            response->set_token(token);
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        virtual void UserLoginByToken(::google::protobuf::RpcController* controller,
            const ::messageSystem::UserLoginByTokenReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到用户登录请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string token = request->token();
            std::string rid = request->request_id();
            response->set_request_id(rid);
        }
        virtual void GetEmailVerifyCode(::google::protobuf::RpcController* controller,
            const ::messageSystem::EmailVerifyCodeReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string email = request->email();
            std::string rid = request->request_id();
            if (!_mail->IsValidEmail(email))
            {
                LOG_INFO("{} - 邮箱非法(email):{}!", rid, email);
                HandlerError(response, rid, false, "邮箱非法!");
                return;
            }
            std::string code;
            do{
                code = _mail->generateUniqueCode();
            }while(_redis->exists(code));
            auto ret = _mail->SendAuthCodeMail(email, code);
            if (!ret.ok)
            {
                LOG_INFO("{} - 发送邮件验证码失败(error):{}!", rid, ret.error);
                HandlerError(response, rid, false, ret.error);
                return;
            }
            auto redis_rep = _redis->setex(email, code,300);
            if (!redis_rep.status)
            {
                LOG_ERROR("{} - Redis设置失败:{}！", rid, redis_rep.errmsg);
                HandlerError(response, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            response->set_request_id(request->request_id());
            response->set_status(true);
            LOG_DEBUG("获取邮件验证码处理完成！");
        }
        virtual void EmailRegister(::google::protobuf::RpcController* controller,
            const ::messageSystem::EmailRegisterReq* request,
            ::messageSystem::CommRsp* rep,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到邮箱注册请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string email = request->email();
            std::string rid = request->request_id();
            if (!_mail->IsValidEmail(email))
            {
                LOG_INFO("{} - 邮箱非法:{}!", rid, email);
                HandlerError(rep, rid, false, "邮箱非法!");
                return;
            }
            sw::redis::OptionalString code;
            auto get_rep = _redis->get(email, &code);
            if (!get_rep.status || !code || code != request->verify_code())
            {
                LOG_INFO("{} - 验证码错误(email):{}!", rid, email);
                HandlerError(rep, rid, false, "验证码错误!");
                return;
            }
            std::shared_ptr<User> user;
            auto select_rep = _odb->selectByEmail(email, &user);
            if (!select_rep.status)
            {
                LOG_ERROR("{} - 查询用户失败:{}！", rid, select_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            if (user)
            {
                LOG_INFO("{} - 该邮箱已注册过用户(email):{}！", rid, email);
                HandlerError(rep, rid, false, "该邮箱已注册过用户!");
                return;
            }
            user = std::make_shared<User>();
            user->email = email;
            auto insert_rep = _odb->insert(*user);
            if (!insert_rep.status)
            {
                LOG_INFO("{} - MySQL注册用户失败(email):{}！", rid, email);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            ESInsert es;
            es.add("uid", user->uid).add("email", email);
            auto es_rep = es.insert("messageSystem", "user", std::to_string(user->uid));
            if (!es_rep.status)
            {
                LOG_INFO("{} - ES注册用户失败(email):{}！", rid, email);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试");
                return;
            }
            rep->set_status(true);
            rep->set_request_id(rid);
        }
        virtual void EmailLogin(::google::protobuf::RpcController* controller,
            const ::messageSystem::EmailLoginReq* request,
            ::messageSystem::EmailLoginRsp* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到邮箱登录请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string email = request->email();
            std::string rid = request->request_id();
            CommRsp*rep = response->mutable_response();
            if (!_mail->IsValidEmail(email))
            {
                LOG_INFO("{} - 邮箱非法(email):{}!", rid, email);
                HandlerError(rep, rid, false, "邮箱非法!");
                return;
            }
            sw::redis::OptionalString code;
            auto get_rep = _redis->get(email, &code);
            if (!get_rep.status || !code || code != request->verify_code())
            {
                LOG_INFO("{} - 验证码错误!", rid);
                HandlerError(rep, rid, false, "验证码错误!");
                return;
            }
            std::shared_ptr<User> user;
            auto select_rep = _odb->selectByEmail(email, &user);
            if (!select_rep.status || !user)
            {
                LOG_INFO("{} - 该邮箱未注册过用户(email):{}！", rid, email);
                HandlerError(rep, rid, false, "该邮箱未注册过用户!");
                return;
            }
            _redis->remove(email);
            if (_redis->exists(std::to_string(user->uid)))
            {
                LOG_INFO("{} - 当前账户已在其他地方登录(uid):{}！", rid, user->uid);
                HandlerError(rep, rid, false, "当前账户已在其他地方登录!");
                return;
            }
            std::string ssid = util::StringUtil::generateUniqueName();
            auto redis_rep = _redis->setex(std::to_string(user->uid), ssid);
            if (!redis_rep.status)
            {
                LOG_ERROR("{} - Redis设置失败:{}！", rid, redis_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            std::string token = util::StringUtil::generateUniqueName();
            redis_rep = _redis->setex("token:uid", token,3600);
            if(!redis_rep.status)
            {
                LOG_ERROR("{} - Redis设置失败:{}！", rid, redis_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            response->set_token(token);
            rep->set_status(true);
            rep->set_request_id(rid);
        }
        virtual void GetMultiUserInfo(::google::protobuf::RpcController* controller,
            const ::messageSystem::GetMultiUserInfoReq* request,
            ::messageSystem::GetMultiUserInfoRsp* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到批量用户信息获取请求！");
            brpc::ClosureGuard rpc_guard(done);
            CommRsp* rep = response->mutable_response();
            std::vector<std::string> uid_lists;
            std::string rid = request->request().request_id();
            uid_lists.resize(request->users_id_size());
            for (int i = 0; i < request->users_id_size(); i++)
            {
                uid_lists[i] = request->users_id(i);
            }
            auto users = _odb->selectMultiById(uid_lists);
            if (users.size() != request->users_id_size())
            {
                LOG_INFO("{} - 从数据库查找的用户信息数量不一致:{}-{}！",
                    rid, request->users_id_size(), users.size());
                HandlerError(rep, rid, false, "未找到当前用户!");
                return;
            }
            ServiceChannel::ChannelPtr channel;
            Response return_rep = _services->chooseService(FILE_SERVICE, &channel);
            if (!return_rep.status)
            {
                LOG_ERROR("{} - {}",rid,return_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            messageSystem::FileService_Stub stub(channel.get());
            messageSystem::GetFileReq req;
            messageSystem::GetFileRsp rsp;
            req.set_request_id(rid);
            for (auto &user : users)
            {
                req.add_file_id_list(user.avatar);
            }
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed() || !rsp.success())
            {
                LOG_INFO("{} - 文件服务异常！", rid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            for (auto &user : users)
            {
                auto user_map = response->mutable_users_info();
                auto file_map = rsp.mutable_file_data();
                UserInfo user_info;
                user_info.set_user_id(std::to_string(user.uid));
                user_info.set_nickname(user.name);
                user_info.set_description(user.desc);
                user_info.set_email(user.email);
                user_info.set_avatar((*file_map)[user.avatar].file_content());
                (*user_map)[user_info.user_id()] = user_info;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        virtual void SetUserAvatar(::google::protobuf::RpcController* controller,
            const ::messageSystem::SetUserAvatarReq* request,
            ::messageSystem::CommRsp* rep,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到用户头像设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string uid = request->request().uid();
            std::string rid = request->request().request_id();
            std::string avatar_id = util::StringUtil::generateUniqueName();
            std::shared_ptr<User> user;
            auto select_rep = _odb->selectById(uid, &user);
            if (!select_rep.status || !user)
            {
                LOG_INFO("{} - 用户不存在(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "当前用户不存在!");
                return;
            }
            ServiceChannel::ChannelPtr channel;
            Response return_rep = _services->chooseService(FILE_SERVICE, &channel);
            if (!return_rep.status)
            {
                LOG_ERROR("{} - {}",rid,return_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            messageSystem::FileService_Stub stub(channel.get());
            messageSystem::PutFileReq req;
            messageSystem::CommRsp rsp;
            req.set_request_id(request->request_id());
            auto data = req.add_file_data();
            data->set_file_name(avatar_id);
            data->set_file_size(request->avatar().size());
            data->set_file_content(request->avatar());
            brpc::Controller cntl;
            stub.PutSingleFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed() || !rsp.status())
            {
                LOG_INFO("{} - 文件管理服务异常！", request->request_id());
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            user->avatar = avatar_id;
            auto update_rep = _odb->update(user);
            if (!update_rep.status)
            {
                LOG_INFO("{} - 更新MySQL用户头像失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            ESInsert es;
            es
            .add("uid", user->uid)
            .add("name", user->name)
            .add("email", user->email)
            .add("desc", user->desc)
            .add("avatar", user->avatar);
            auto es_rep = es.insert("messageSystem", "user", uid);
            if (!es_rep.status)
            {
                LOG_INFO("{} - 更新ES失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        virtual void SetUserNickname(::google::protobuf::RpcController* controller,
            const ::messageSystem::SetUserNameReq* request,
            ::messageSystem::CommRsp* rep,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到用户昵称设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string uid = request->request().uid();
            std::string new_name = request->nickname();
            if (!checkName(new_name))
            {
                LOG_INFO("{} - 用户名非法(name):{}！", rid, new_name);
                HandlerError(rep, rid, false, "用户名非法!");
                return;
            }
            std::shared_ptr<User> user;
            auto select_rep = _odb->selectById(uid, &user);
            if (!select_rep.status || !user)
            {
                LOG_INFO("{} - 未找到该用户(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "未找到该用户!");
                return;
            }
            user->name = new_name;
            auto update_rep = _odb->update(user);
            if (!update_rep.status)
            {
                LOG_INFO("{} - 更新MySQL用户名称失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            ESInsert es;
            es
            .add("uid", user->uid)
            .add("name", user->name)
            .add("email", user->email)
            .add("desc", user->desc)
            .add("avatar", user->avatar);
            auto es_rep = es.insert("messageSystem", "user", uid);
            if (!es_rep.status)
            {
                LOG_INFO("{} - 更新ES失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        virtual void SetUserDescription(::google::protobuf::RpcController* controller,
            const ::messageSystem::SetUserDescriptionReq* request,
            ::messageSystem::CommRsp* rep,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到用户签名设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string uid = request->request().uid();
            std::string new_desc = request->desc();
            std::shared_ptr<User> user;
            auto select_rep = _odb->selectById(uid, &user);
            if (!select_rep.status || !user)
            {
                LOG_INFO("{} - 未找到该用户(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "未找到该用户!");
                return;
            }
            user->desc = new_desc;
            auto update_rep = _odb->update(user);
            if (!update_rep.status)
            {
                LOG_INFO("{} - 更新MySQL用户签名失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            ESInsert es;
            es
            .add("uid", user->uid)
            .add("name", user->name)
            .add("email", user->email)
            .add("desc", user->desc)
            .add("avatar", user->avatar);
            auto es_rep = es.insert("messageSystem", "user", uid);
            if (!es_rep.status)
            {
                LOG_INFO("{} - 更新ES失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        virtual void SetUserEmailNumber(::google::protobuf::RpcController* controller,
            const ::messageSystem::SetUserEmailReq* request,
            ::messageSystem::CommRsp* rep,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到用户邮箱设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string uid = request->request().uid();
            std::string new_email = request->email();
            std::string code = request->email_verify_code();
            sw::redis::OptionalString vcode;
            auto get_rep = _redis->get(uid, &vcode);
            if (!get_rep.status || !vcode|| vcode != code)
            {
                LOG_INFO("{} - 验证码错误！", rid);
                HandlerError(rep, rid, false, "验证码错误!");
                return;
            }
            std::shared_ptr<User> user;
            auto select_rep = _odb->selectById(uid, &user);
            if (!select_rep.status || !user)
            {
                LOG_INFO("{} - 未找到该用户(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "未找到该用户!");
                return;
            }
            user->email = new_email;
            auto update_rep = _odb->update(user);
            if (!update_rep.status)
            {
                LOG_INFO("{} - 更新MySQL用户邮箱失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            ESInsert es;
            es
            .add("uid", user->uid)
            .add("name", user->name)
            .add("email", user->email)
            .add("desc", user->desc)
            .add("avatar", user->avatar);
            auto es_rep = es.insert("messageSystem", "user", uid);
            if (!es_rep.status)
            {
                LOG_INFO("{} - 更新ES失败(uid):{}！", rid, uid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
    private:
        std::shared_ptr<odbUser::OdbUser> _odb;
        std::shared_ptr<redis::RedisClient> _redis;
        std::shared_ptr<Mail> _mail;
        std::shared_ptr<ServiceManager> _services;
    };
    class UserServer
    {
    private:
        void Put(const std::string& key,const std::string& value)
        {
            if(key == _file_service_base_url)
            {
                _services->addService(FILE_SERVICE, value);
                _services->activeService(key);
            }
        }
        void Del(const std::string& key,const std::string& value)
        {
            if(key == _file_service_base_url)
            {
                _services->inactiveService(key);
            }
        }
    public:
        UserServer(const std::string& file_service_base_url,const std::string& host,const std::string& basedir)
        :_file_service_base_url(file_service_base_url)
        ,_discover(host,basedir,std::bind(&UserServer::Put,this,std::placeholders::_1,std::placeholders::_2)
                               ,std::bind(&UserServer::Del,this,std::placeholders::_1,std::placeholders::_2))
        {
            _services = std::make_shared<ServiceManager>();
            _server.InitService(_services);
        }
        void InitODB(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        {
            std::shared_ptr<odbUser::OdbUser> odb = std::make_shared<odbUser::OdbUser>(user,password,db,host,port);
            _server.InitODB(odb);
        }
        void InitRedis(const std::string& ip,int port,int thread_size,int late_time)
        {
            std::shared_ptr<redis::RedisClient> redis = std::make_shared<redis::RedisClient>(ip,port,thread_size,late_time);
            _server.InitRedis(redis);
        }
        void Start(uint16_t port, int32_t timeout, uint8_t num_threads)
        {
            _rpc_server = std::make_unique<brpc::Server>();
            int ret = _rpc_server->AddService(&_server,brpc::ServiceOwnership::SERVER_OWNS_SERVICE);
            if(ret == -1)
            {
                LOG_ERROR("添加RPC服务失败!");
                return;
            }
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout;
            options.num_threads = num_threads;
            ret = _rpc_server->Start(port,&options);
            if(ret == -1)
            {
                LOG_ERROR("服务启动失败!");
                return;
            }
        }
    private:
        std::unique_ptr<brpc::Server> _rpc_server;
        std::string _file_service_base_url;
        Discovery _discover;
        UserServiceImpl _server;
        std::shared_ptr<ServiceManager> _services;
    };
}
