#include <httplib.h>
#include <memory>
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
        }
    public:
        virtual void GetEmailVerifyCode(::google::protobuf::RpcController* controller,
            ::messageSystem::EmailVerifyCodeReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            std::string* rid = new string(util::StringUtil::generateUniqueName());
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
            std::string* rid = new string(util::StringUtil::generateUniqueName());
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
            std::string* rid = new string(util::StringUtil::generateUniqueName());
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
    private:
        std::shared_ptr<Discovery> discover;
        std::shared_ptr<ServiceManager> services;
        std::unordered_map<std::string, std::string> _sessions;
    };
}