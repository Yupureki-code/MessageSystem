#include <httplib.h>
#include <memory>
#include <string>
#include "../../comm_include/channel.hpp"
#include "../../comm_include/proto_include/gateway.pb.h"
#include "../../comm_include/comm.hpp"
#include <brpc/callback.h>
#include <brpc/controller.h>
#include <functional>
#include <sw/redis++/utils.h>
#include <unordered_map>
#include "../../comm_include/etcd.hpp"
#include "../../comm_include/redis.hpp"
#include "ws_server.hpp"

namespace messageSystem
{
    class GateWayServerImpl : public GateWayServer
    {
    private:
        template<class T>
        using AsyncCallBack = std::function<void(bool status,T* response)>;
        void HandlerError(CommRsp* rep,bool status,const std::string& msg = "服务器繁忙，请稍后重试!")
        {
            rep->set_status(status);
            if(!msg.empty())
                rep->set_errmsg(msg);
        }
        template<class T>
        void asyncDone(brpc::Controller* cntl, T* response, AsyncCallBack<T> cb,::google::protobuf::Closure* done,std::string* rid)
        {
            if(cntl->Failed())
            {
                LOG_ERROR("{} - {}:{}",*rid,cntl->ErrorCode(),cntl->ErrorText());
            }
            if(cb)
                cb(!cntl->Failed(),response);
            delete cntl;
            delete rid;
            done->Run();
        }
        bool CheckToken(const std::string& uid,const std::string token,CommRsp* rsp)
        {
            sw::redis::OptionalString value;
            auto rep = _redis->get("token:" + uid, &value);
            if(!rep.status || !value || token != value)
            {
                HandlerError(rsp, false,"登陆状态过期!");
                return false;
            }
            return true;
        }
    public:
        GateWayServerImpl(std::shared_ptr<ServiceManager> services,
        std::shared_ptr<redis::RedisClient> redis,int port)
        :_services(services),_redis(redis)
        {
            _ws_server = std::make_shared<WsServer>(port);
        }
        virtual void GetEmailVerifyCode(::google::protobuf::RpcController* controller,
            ::messageSystem::EmailVerifyCodeReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.GetEmailVerifyCode(cntl, request, response, async_done);
        }
        virtual void EmailRegister(::google::protobuf::RpcController* controller,
            ::messageSystem::EmailRegisterReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status)response->set_status(true);
                else HandlerError(response, false,"");
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.EmailRegister(cntl,request,response,async_done);
        }
        virtual void EmailLogin(::google::protobuf::RpcController* controller,
            ::messageSystem::EmailLoginReq* request,
            ::messageSystem::EmailLoginRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            std::string uid = request->uid();
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<EmailLoginRsp> cb = [this,uid](bool status,EmailLoginRsp* response){
                if(status)
                {
                    response->mutable_response()->set_status(true);
                }
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<EmailLoginRsp>,cntl,response,cb,done,rid);
            stub.EmailLogin(cntl,request,response,async_done);
        }
        virtual void UserLogin(::google::protobuf::RpcController* controller,
            ::messageSystem::UserLoginReq* request,
            ::messageSystem::UserLoginRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            std::string uid = request->uid();
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<UserLoginRsp> cb = [this,uid](bool status,UserLoginRsp* response){
                if(status)
                {
                    response->mutable_response()->set_status(true);
                }
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<UserLoginRsp>,cntl,response,cb,done,rid);
            stub.UserLogin(cntl,request,response,async_done);
        }
        virtual void GetUserInfo(::google::protobuf::RpcController* controller,
            ::messageSystem::GetUserInfoReq* request,
            ::messageSystem::GetUserInfoRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            CommRsp* rep = response->mutable_response();
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, rep))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<GetUserInfoRsp> cb = [this](bool status,GetUserInfoRsp* response){
                if(status) response->mutable_response()->set_status(true);
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<GetUserInfoRsp>,cntl,response,cb,done,rid);
            stub.GetUserInfo(cntl,request,response,async_done);
        }
        virtual void GetMultiUserInfo(::google::protobuf::RpcController* controller,
            ::messageSystem::GetMultiUserInfoReq* request,
            ::messageSystem::GetMultiUserInfoRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            CommRsp* rep = response->mutable_response();
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, rep))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<GetMultiUserInfoRsp> cb = [this](bool status,GetMultiUserInfoRsp* response){
                if(status) response->mutable_response()->set_status(true);
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<GetMultiUserInfoRsp>,cntl,response,cb,done,rid);
            stub.GetMultiUserInfo(cntl,request,response,async_done);
        }
        virtual void SetUserAvatar(::google::protobuf::RpcController* controller,
            ::messageSystem::SetUserAvatarReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.SetUserAvatar(cntl,request,response,async_done);
        }
        virtual void SetUserNickname(::google::protobuf::RpcController* controller,
            ::messageSystem::SetUserNameReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.SetUserNickname(cntl,request,response,async_done);
        }
        virtual void SetUserDescription(::google::protobuf::RpcController* controller,
            ::messageSystem::SetUserDescriptionReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.SetUserDescription(cntl,request,response,async_done);
        }
        virtual void SetUserEmail(::google::protobuf::RpcController* controller,
            ::messageSystem::SetUserEmailReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.SetUserEmail(cntl,request,response,async_done);
        }
        virtual void CreateConversation(::google::protobuf::RpcController* controller,
            ::messageSystem::CreateConversationReq* request,
            ::messageSystem::CreateConversationRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            CommRsp* rep = response->mutable_response();
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, rep))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(CONVERSATION_SERVICE, &channel);
            ConversationServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CreateConversationRsp> cb = [this](bool status,CreateConversationRsp* response){
                if(status) response->mutable_response()->set_status(true);
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CreateConversationRsp>,cntl,response,cb,done,rid);
            stub.CreateConversation(cntl,request,response,async_done);
        }
        virtual void RemoveConversation(::google::protobuf::RpcController* controller,
            ::messageSystem::RemoveConversationReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(CONVERSATION_SERVICE, &channel);
            ConversationServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.RemoveConversation(cntl,request,response,async_done);
        }
        virtual void AddMember(::google::protobuf::RpcController* controller,
            ::messageSystem::AddMemberReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(CONVERSATION_SERVICE, &channel);
            ConversationServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.AddMember(cntl,request,response,async_done);
        }
        virtual void ExitConversation(::google::protobuf::RpcController* controller,
            ::messageSystem::ExitConversationReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(CONVERSATION_SERVICE, &channel);
            ConversationServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.ExitConversation(cntl,request,response,async_done);
        }
        virtual void ChangeMemberPower(::google::protobuf::RpcController* controller,
            ::messageSystem::ChangeMemberPowerReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(CONVERSATION_SERVICE, &channel);
            ConversationServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.ChangeMemberPower(cntl,request,response,async_done);
        }
        virtual void GetConversationMemberList(::google::protobuf::RpcController* controller,
            ::messageSystem::GetConversationMemberListReq* request,
            ::messageSystem::GetConversationMemberListRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            CommRsp* rep = response->mutable_response();
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, rep))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(CONVERSATION_SERVICE, &channel);
            ConversationServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<GetConversationMemberListRsp> cb = [this](bool status,GetConversationMemberListRsp* response){
                if(status) response->mutable_response()->set_status(true);
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<GetConversationMemberListRsp>,cntl,response,cb,done,rid);
            stub.GetConversationMemberList(cntl,request,response,async_done);
        }
        virtual void SearchConversation(::google::protobuf::RpcController* controller,
            ::messageSystem::SearchConversationReq* request,
            ::messageSystem::SearchConversationRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            CommRsp* rep = response->mutable_response();
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, rep))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(CONVERSATION_SERVICE, &channel);
            ConversationServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<SearchConversationRsp> cb = [this](bool status,SearchConversationRsp* response){
                if(status) response->mutable_response()->set_status(true);
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<SearchConversationRsp>,cntl,response,cb,done,rid);
            stub.SearchConversation(cntl,request,response,async_done);
        }
        virtual void FriendRequest(::google::protobuf::RpcController* controller,
            ::messageSystem::FriendRequestReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(FRIEND_SERVICE, &channel);
            FriendServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.FriendRequest(cntl,request,response,async_done);
        }
        virtual void FriendRequestStatus(::google::protobuf::RpcController* controller,
            ::messageSystem::FriendRequestStatusReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(FRIEND_SERVICE, &channel);
            FriendServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.FriendRequestStatus(cntl,request,response,async_done);
        }
        virtual void FriendRemark(::google::protobuf::RpcController* controller,
            ::messageSystem::FriendRemarkReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, response))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(FRIEND_SERVICE, &channel);
            FriendServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<CommRsp> cb = [this](bool status,CommRsp* response){
                if(status) response->set_status(true);
                else HandlerError(response, false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<CommRsp>,cntl,response,cb,done,rid);
            stub.FriendRemark(cntl,request,response,async_done);
        }
        virtual void FindFriendByUID(::google::protobuf::RpcController* controller,
            ::messageSystem::FindFriendByUIDReq* request,
            ::messageSystem::FindFriendByUIDRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            CommRsp* rep = response->mutable_response();
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, rep))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(FRIEND_SERVICE, &channel);
            FriendServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<FindFriendByUIDRsp> cb = [this](bool status,FindFriendByUIDRsp* response){
                if(status) response->mutable_response()->set_status(true);
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<FindFriendByUIDRsp>,cntl,response,cb,done,rid);
            stub.FindFriendByUID(cntl,request,response,async_done);
        }
        virtual void FindFriendByName(::google::protobuf::RpcController* controller,
            ::messageSystem::FindFriendByNameReq* request,
            ::messageSystem::FindFriendByNameRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            CommRsp* rep = response->mutable_response();
            // 从CommReq获取uid和token进行验证
            std::string uid = request->request().uid();
            std::string token = request->request().token();
            if(!CheckToken(uid, token, rep))
            {
                delete rid;
                return;
            }
            request->mutable_request()->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            _services->chooseService(FRIEND_SERVICE, &channel);
            FriendServer_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<FindFriendByNameRsp> cb = [this](bool status,FindFriendByNameRsp* response){
                if(status) response->mutable_response()->set_status(true);
                else HandlerError(response->mutable_response(), false);
            };
            google::protobuf::Closure* async_done = brpc::NewCallback(this,&GateWayServerImpl::asyncDone<FindFriendByNameRsp>,cntl,response,cb,done,rid);
            stub.FindFriendByName(cntl,request,response,async_done);
        }
        
    private:
        std::shared_ptr<ServiceManager> _services;
        std::shared_ptr<WsServer> _ws_server;
        std::shared_ptr<redis::RedisClient> _redis;
    };
    class GatwayServer
    {
    private:
        void Put(const std::string& key, const std::string& value)
        {
            auto it = _service_keys.find(key);
            if(it == _service_keys.end())
                return;
            _services->addService(it->second, value);
            _services->activeService(it->second);
        }
        void Del(const std::string& key, const std::string& value)
        {
            auto it = _service_keys.find(key);
            if(it == _service_keys.end())
                return;
            _services->inactiveService(it->second);
        }
    public:
        GatwayServer(const std::string& host, const std::string& basedir)
        :_discover(host, basedir, std::bind(&GatwayServer::Put, this, std::placeholders::_1, std::placeholders::_2)
                                   , std::bind(&GatwayServer::Del, this, std::placeholders::_1, std::placeholders::_2))
        {
            _services = std::make_shared<ServiceManager>();
            _service_keys[SERVICE_BASE_URL + FILE_SERVICE] = FILE_SERVICE;
            _service_keys[SERVICE_BASE_URL + USER_SERVICE] = USER_SERVICE;
            _service_keys[SERVICE_BASE_URL + CONVERSATION_SERVICE] = CONVERSATION_SERVICE;
            _service_keys[SERVICE_BASE_URL + FRIEND_SERVICE] = FRIEND_SERVICE;
            _service_keys[SERVICE_BASE_URL + MESSAGE_SERVICE] = MESSAGE_SERVICE;
            _service_keys[SERVICE_BASE_URL + MESSAGE_STORE_SERVICE] = MESSAGE_STORE_SERVICE;
        }
        void InitRedis(const std::string& ip,int port,int thread_size,int late_time)
        {
            _redis = std::make_shared<redis::RedisClient>(ip,port,thread_size,late_time);
        }
        void Start(int port, int32_t timeout, uint8_t num_threads)
        {
            GateWayServerImpl server(_services,_redis,port);
            brpc::Server _rpc_server;
            int ret = _rpc_server.AddService(&server,brpc::ServiceOwnership::SERVER_OWNS_SERVICE);
            if(ret == -1)
            {
                LOG_ERROR("添加RPC服务失败!");
                return;
            }
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout;
            options.num_threads = num_threads;
            ret = _rpc_server.Start(port,&options);
            if(ret == -1)
            {
                LOG_ERROR("服务启动失败!");
                return;
            }
        }
    private:
        Discovery _discover;
        std::shared_ptr<ServiceManager> _services;
        std::shared_ptr<redis::RedisClient> _redis;
        std::unordered_map<std::string, std::string> _service_keys;
    };
}