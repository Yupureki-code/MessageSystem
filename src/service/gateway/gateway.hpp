#include <httplib.h>
#include <memory>
#include <string>
#include "../../comm_include/proto_include/gateway.pb.h"
#include "../../comm_include/comm.hpp"
#include <brpc/callback.h>
#include <brpc/controller.h>
#include <functional>
#include <unordered_map>
#include "../../comm_include/etcd.hpp"
#include "../../comm_include/channel.hpp"

namespace messageSystem
{
    class GateWayServerImpl : GateWayServer
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
    public:
        virtual void GetEmailVerifyCode(::google::protobuf::RpcController* controller,
            ::messageSystem::EmailVerifyCodeReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new std::string(util::StringUtil::generateUniqueName());
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(USER_SERVICE, &channel);
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
            services->chooseService(USER_SERVICE, &channel);
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
            services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<EmailLoginRsp> cb = [this,uid](bool status,EmailLoginRsp* response){
                if(status)
                {
                    response->mutable_response()->set_status(true);
                    _sessions[uid] = response->session_id();
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
            services->chooseService(USER_SERVICE, &channel);
            UserService_Stub stub(channel.get());
            brpc::Controller* cntl = new brpc::Controller();
            AsyncCallBack<UserLoginRsp> cb = [this,uid](bool status,UserLoginRsp* response){
                if(status)
                {
                    response->mutable_response()->set_status(true);
                    _sessions[uid] = response->session_id();
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response->mutable_response(), false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(USER_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response->mutable_response(), false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(USER_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(USER_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(USER_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(USER_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(USER_SERVICE, &channel);
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
            std::string uid = request->owner_uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response->mutable_response(), false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(CONVERSATION_SERVICE, &channel);
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
            std::string uid = request->owner_uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(CONVERSATION_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(CONVERSATION_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(CONVERSATION_SERVICE, &channel);
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
            std::string uid = request->owner_uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(CONVERSATION_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response->mutable_response(), false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(CONVERSATION_SERVICE, &channel);
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
            std::string uid = request->user_id();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response->mutable_response(), false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(CONVERSATION_SERVICE, &channel);
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
            std::string uid = request->uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(FRIEND_SERVICE, &channel);
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
            std::string uid = request->uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(FRIEND_SERVICE, &channel);
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
            std::string uid = request->uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response, false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(FRIEND_SERVICE, &channel);
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
            std::string uid = request->uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response->mutable_response(), false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(FRIEND_SERVICE, &channel);
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
            std::string uid = request->uid();
            std::string sid = request->session_id();
            // 检查session_id映射
            if(_sessions.find(uid) == _sessions.end() || _sessions[uid] != sid)
            {
                HandlerError(response->mutable_response(), false, "用户未登录或session无效!");
                delete rid;
                return;
            }
            request->set_request_id(*rid);
            ServiceChannel::ChannelPtr channel;
            services->chooseService(FRIEND_SERVICE, &channel);
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
        std::shared_ptr<Discovery> discover;
        std::shared_ptr<ServiceManager> services;
        std::unordered_map<std::string, std::string> _sessions;
        std::unordered_map<std::string, typename Tp>
    };
}