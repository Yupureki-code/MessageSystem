#pragma once
#include "ws_protocol.hpp"
#include "ws_router.hpp"
#include "ws_connection.hpp"
#include "ws_backend_pool.hpp"
#include "../../comm_include/channel.hpp"
#include "../../comm_include/proto_include/gateway.pb.h"
#include "../../comm_include/proto_include/message.pb.h"
#include "../../comm_include/redis.hpp"
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <memory>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <sw/redis++/utils.h>
#include <unordered_map>
#include "../../comm_include/comm.hpp"

namespace messageSystem
{

    enum class WsCloseCode : std::uint16_t 
    {
        NormalClosure       = 1000,
        GoingAway           = 1001,
        ProtocolError       = 1002,
        UnsupportedData     = 1003,
        NoStatusReceived    = 1005,
        AbnormalClosure     = 1006,
        InvalidPayloadData  = 1007,
        PolicyViolation     = 1008,
        MessageTooBig       = 1009,
        MissingExtension    = 1010,
        InternalError       = 1011,
        ServiceRestart      = 1012,
        TryAgainLater       = 1013,
        TLSHandshakeFailure = 1015,
        Unauthorized        = 4001,
        DuplicateConnection = 4002,
        RateLimitExceeded   = 4003,
    };
    class WsHandlers
    {
    private:
        bool CheckSessionID(const WsProtocolReq& msg)
        {
            std::lock_guard<std::mutex> lock(_session_mutex);
            auto it = _sessions.find(msg.user_id);
            if(it == _sessions.end() || it->second != msg.session_id)
                return false;
            return true;
        }
        std::string GetSessionToken(const std::string& user_id)
        {
            std::lock_guard<std::mutex> lock(_session_mutex);
            auto it = _sessions.find(user_id);
            if(it == _sessions.end())
                return "";
            return it->second;
        }
        void HandlerError(WsProtocolRsp& msg,WsCloseCode status_code)
        {
            int code = static_cast<int>(status_code);
            msg.code = code;
            switch (code) 
            {
                case static_cast<int>(WsCloseCode::Unauthorized):
                    msg.error = "用户未登录或session无效!";
                    break;
                case static_cast<int>(WsCloseCode::ProtocolError):
                    msg.error = "协议解析错误!";
                    break;
                default:
                    msg.error = "服务器繁忙，请稍后重试!";
                    break;
            }
        }
        /// @brief 回复客户端
        boost::asio::awaitable<void> replyToClient(std::shared_ptr<Connection> conn, const WsProtocolRsp& rsp)
        {
            co_await conn->ws->async_write(net::buffer(rsp.serialize()), net::use_awaitable);
        }
        /// @brief 广播Notify到指定用户列表（排除发送者）
        void broadcastNotify(const std::vector<std::string>& uids, const WsProtocolReq& notify, const std::string& exclude_uid = "")
        {
            for(const auto& uid : uids)
            {
                if(uid == exclude_uid) continue;
                auto c = _manager.getByUid(uid);
                if(!c || !c->authenticated) continue;
                std::string data = notify.serialize();
                net::post(_ioc, [c, data]() {
                    c->ws->async_write(net::buffer(data), [](boost::beast::error_code, std::size_t) {});
                });
            }
        }
    public:
        /// @brief Auth 认证登录
        boost::asio::awaitable<void> Auth(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_AUTH;
            auto j = nlohmann::json::parse(req.payload.empty() ? "{}" : req.payload);
            std::string uid = req.user_id;
            std::string token = j.value("token","");
            sw::redis::OptionalString get_token;
            auto rep = _redis->get("token:" + uid, &get_token);
            if(!rep.status || !get_token || get_token.value() != token)
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            std::string session_id = util::StringUtil::generateUniqueName();
            {
                std::lock_guard<std::mutex> lock(_session_mutex);
                _sessions[uid] = session_id;
            }
            _manager.authenticate(conn_id, uid);
            response.code = 1000;
            response.payload = session_id;
            co_await replyToClient(conn, response);
        }
        /// @brief SendMessage 发送消息
        boost::asio::awaitable<void> SendMessage(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_SEND_MESSAGE;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::SendMessage msg;
            if(!msg.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            // 回复发送者
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播Notify给会话成员
            WsProtocolReq notify;
            notify.type = WS_SEND_MESSAGE_NOTIFY;
            notify.payload = msg.message().SerializeAsString();
            // 从消息中提取会话ID，获取成员列表广播
            std::string cid = msg.message().conversation_id();
            // 广播给在线的会话成员（排除发送者）
            // 注意: 实际实现中需要从ES/Redis获取会话成员列表
        }
        /// @brief RecallMessage 撤回消息
        boost::asio::awaitable<void> RecallMessage(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_RECALL_MESSAGE;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播RecallMessageNotify给会话成员
            WsProtocolReq notify;
            notify.type = WS_RECALL_MESSAGE_NOTIFY;
            notify.payload = req.payload;
            // 广播给在线的会话成员（排除发送者）
        }
        /// @brief Typing 正在输入
        boost::asio::awaitable<void> Typing(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_TYPING;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::Typing typing;
            if(!typing.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播TypingNotify
            WsProtocolReq notify;
            notify.type = WS_TYPING_NOTIFY;
            messageSystem::TypingNotify notify_msg;
            notify_msg.set_uid(req.user_id);
            notify_msg.set_is_typing(typing.is_typing());
            notify.payload = notify_msg.SerializeAsString();
        }
        /// @brief FriendRequest 好友请求
        boost::asio::awaitable<void> FriendRequest(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_FRIEND_REQUEST;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::FriendRequest friend_req;
            if(!friend_req.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            // 通过brpc调用好友服务
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(FRIEND_SERVICE, &channel);
            if(!rep.status)
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::FriendRequestReq rpc_req;
            messageSystem::CommRsp rpc_rsp;
            rpc_req.mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
            rpc_req.mutable_request()->set_uid(req.user_id);
            rpc_req.mutable_request()->set_token(GetSessionToken(req.user_id));
            rpc_req.set_friend_uid(friend_req.friend_uid());
            rpc_req.set_remark(friend_req.remark());
            FriendServer_Stub stub(channel.get());
            co_await runInLoop(nullptr, _ioc, [&stub, &rpc_req, &rpc_rsp](::google::protobuf::Closure *done){
                brpc::Controller cntl;
                stub.FriendRequest(&cntl, &rpc_req, &rpc_rsp, done);
            });
            if(!rpc_rsp.status())
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播FriendRequestNotify给对方
            WsProtocolReq notify;
            notify.type = WS_FRIEND_REQUEST_NOTIFY;
            messageSystem::FriendRequestNotify notify_msg;
            notify_msg.set_friend_uid(req.user_id);
            notify_msg.set_remark(friend_req.remark());
            notify.payload = notify_msg.SerializeAsString();
            broadcastNotify({friend_req.friend_uid()}, notify);
        }
        /// @brief FriendRequestStatus 好友请求状态更新
        boost::asio::awaitable<void> FriendRequestStatus(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_FRIEND_REQUEST_STATUS;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::FriendRequestStatus status_req;
            if(!status_req.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(FRIEND_SERVICE, &channel);
            if(!rep.status)
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::FriendRequestStatusReq rpc_req;
            messageSystem::CommRsp rpc_rsp;
            rpc_req.mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
            rpc_req.mutable_request()->set_uid(req.user_id);
            rpc_req.mutable_request()->set_token(GetSessionToken(req.user_id));
            rpc_req.set_friend_uid(status_req.friend_uid());
            rpc_req.set_is_accepcted(status_req.is_accepted());
            FriendServer_Stub stub(channel.get());
            co_await runInLoop(nullptr, _ioc, [&stub, &rpc_req, &rpc_rsp](::google::protobuf::Closure *done){
                brpc::Controller cntl;
                stub.FriendRequestStatus(&cntl, &rpc_req, &rpc_rsp, done);
            });
            if(!rpc_rsp.status())
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播FriendRequestStatusNotify给对方
            WsProtocolReq notify;
            notify.type = WS_FRIEND_REQUEST_STATUS_NOTIFY;
            messageSystem::FriendRequestStatusNotify notify_msg;
            notify_msg.set_friend_uid(req.user_id);
            notify_msg.set_is_accepted(status_req.is_accepted());
            notify.payload = notify_msg.SerializeAsString();
            broadcastNotify({status_req.friend_uid()}, notify);
        }
        /// @brief CreateConversation 创建会话
        boost::asio::awaitable<void> CreateConversation(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_CREATE_CONVERSATION;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::CreateConversation conv_req;
            if(!conv_req.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(CONVERSATION_SERVICE, &channel);
            if(!rep.status)
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::CreateConversationReq rpc_req;
            messageSystem::CreateConversationRsp rpc_rsp;
            rpc_req.mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
            rpc_req.mutable_request()->set_uid(req.user_id);
            rpc_req.mutable_request()->set_token(GetSessionToken(req.user_id));
            rpc_req.set_is_group(conv_req.is_group());
            for(const auto& uid : conv_req.comm_uid())
                rpc_req.add_comm_uid(uid);
            if(conv_req.has_group_conversation_name())
                rpc_req.set_group_conversation_name(conv_req.group_conversation_name());
            ConversationServer_Stub stub(channel.get());
            co_await runInLoop(nullptr, _ioc, [&stub, &rpc_req, &rpc_rsp](::google::protobuf::Closure *done){
                brpc::Controller cntl;
                stub.CreateConversation(&cntl, &rpc_req, &rpc_rsp, done);
            });
            if(!rpc_rsp.response().status())
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            response.payload = rpc_rsp.conversation_id();
            co_await replyToClient(conn, response);
            // 广播CreateConversationNotify给所有成员(包括自己)
            WsProtocolReq notify;
            notify.type = WS_CREATE_CONVERSATION_NOTIFY;
            messageSystem::CreateConversationNotify notify_msg;
            notify_msg.set_conversation_id(rpc_rsp.conversation_id());
            notify.payload = notify_msg.SerializeAsString();
            std::vector<std::string> comm_uids = {conv_req.comm_uid().begin(), conv_req.comm_uid().end()};
            comm_uids.push_back(req.user_id);
            broadcastNotify(comm_uids, notify);
        }
        /// @brief RemoveConversation 删除会话
        boost::asio::awaitable<void> RemoveConversation(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_REMOVE_CONVERSATION;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::RemoveConversation conv_req;
            if(!conv_req.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(CONVERSATION_SERVICE, &channel);
            if(!rep.status)
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::RemoveConversationReq rpc_req;
            messageSystem::CommRsp rpc_rsp;
            rpc_req.mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
            rpc_req.mutable_request()->set_uid(req.user_id);
            rpc_req.mutable_request()->set_token(GetSessionToken(req.user_id));
            rpc_req.set_conversaion_id(conv_req.conversation_id());
            ConversationServer_Stub stub(channel.get());
            co_await runInLoop(nullptr, _ioc, [&stub, &rpc_req, &rpc_rsp](::google::protobuf::Closure *done){
                brpc::Controller cntl;
                stub.RemoveConversation(&cntl, &rpc_req, &rpc_rsp, done);
            });
            if(!rpc_rsp.status())
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播RemoveConversationNotify给会话成员（排除自己）
            WsProtocolReq notify;
            notify.type = WS_REMOVE_CONVERSATION_NOTIFY;
            messageSystem::RemoveConversationNotify notify_msg;
            notify_msg.set_conversation_id(conv_req.conversation_id());
            notify.payload = notify_msg.SerializeAsString();
            // 从ES获取成员列表进行广播
        }
        /// @brief AddMember 添加成员
        boost::asio::awaitable<void> AddMember(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_ADD_MEMBER;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::AddMember member_req;
            if(!member_req.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(CONVERSATION_SERVICE, &channel);
            if(!rep.status)
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::AddMemberReq rpc_req;
            messageSystem::CommRsp rpc_rsp;
            rpc_req.mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
            rpc_req.mutable_request()->set_uid(req.user_id);
            rpc_req.mutable_request()->set_token(GetSessionToken(req.user_id));
            rpc_req.set_conversaion_id(member_req.conversation_id());
            ConversationServer_Stub stub(channel.get());
            co_await runInLoop(nullptr, _ioc, [&stub, &rpc_req, &rpc_rsp](::google::protobuf::Closure *done){
                brpc::Controller cntl;
                stub.AddMember(&cntl, &rpc_req, &rpc_rsp, done);
            });
            if(!rpc_rsp.status())
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播AddMemberNotify给会话成员（含新成员）
            WsProtocolReq notify;
            notify.type = WS_ADD_MEMBER_NOTIFY;
            messageSystem::AddMemberNotify notify_msg;
            notify_msg.set_conversation_id(member_req.conversation_id());
            notify_msg.set_new_member_uid(member_req.new_member_uid());
            notify.payload = notify_msg.SerializeAsString();
        }
        /// @brief ExitConversation 退出会话
        boost::asio::awaitable<void> ExitConversation(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_EXIT_CONVERSATION;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::ExitConversation exit_req;
            if(!exit_req.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(CONVERSATION_SERVICE, &channel);
            if(!rep.status)
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::ExitConversationReq rpc_req;
            messageSystem::CommRsp rpc_rsp;
            rpc_req.mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
            rpc_req.mutable_request()->set_uid(req.user_id);
            rpc_req.mutable_request()->set_token(GetSessionToken(req.user_id));
            rpc_req.set_conversaion_id(exit_req.conversation_id());
            ConversationServer_Stub stub(channel.get());
            co_await runInLoop(nullptr, _ioc, [&stub, &rpc_req, &rpc_rsp](::google::protobuf::Closure *done){
                brpc::Controller cntl;
                stub.ExitConversation(&cntl, &rpc_req, &rpc_rsp, done);
            });
            if(!rpc_rsp.status())
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 如果是被踢，广播给被踢的人
            if(exit_req.is_kicked() && exit_req.has_the_chosen_one())
            {
                WsProtocolReq notify;
                notify.type = WS_EXIT_CONVERSATION_NOTIFY;
                messageSystem::ExitConversationNotify notify_msg;
                notify_msg.set_uid(exit_req.the_chosen_one());
                notify_msg.set_conversation_id(exit_req.conversation_id());
                notify.payload = notify_msg.SerializeAsString();
                broadcastNotify({exit_req.the_chosen_one()}, notify);
            }
        }
        /// @brief ChangePower 修改权限
        boost::asio::awaitable<void> ChangePower(
            const std::string& conn_id,
            const WsProtocolReq& req,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocolRsp response;
            response.type = WS_CHANGE_POWER;
            if(!CheckSessionID(req))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::ChangePower power_req;
            if(!power_req.ParseFromString(req.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await replyToClient(conn, response);
                co_return;
            }
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(CONVERSATION_SERVICE, &channel);
            if(!rep.status)
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            messageSystem::ChangeMemberPowerReq rpc_req;
            messageSystem::CommRsp rpc_rsp;
            rpc_req.mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
            rpc_req.mutable_request()->set_uid(req.user_id);
            rpc_req.mutable_request()->set_token(GetSessionToken(req.user_id));
            rpc_req.set_conversaion_id(power_req.conversation_id());
            rpc_req.set_uid(power_req.the_chosen_one());
            rpc_req.set_power(power_req.power());
            ConversationServer_Stub stub(channel.get());
            co_await runInLoop(nullptr, _ioc, [&stub, &rpc_req, &rpc_rsp](::google::protobuf::Closure *done){
                brpc::Controller cntl;
                stub.ChangeMemberPower(&cntl, &rpc_req, &rpc_rsp, done);
            });
            if(!rpc_rsp.status())
            {
                HandlerError(response, WsCloseCode::InternalError);
                co_await replyToClient(conn, response);
                co_return;
            }
            response.code = 1000;
            co_await replyToClient(conn, response);
            // 广播ChangePowerNotify给被修改权限的人
            WsProtocolReq notify;
            notify.type = WS_CHANGE_POWER_NOTIFY;
            messageSystem::ChangePowerNotify notify_msg;
            notify_msg.set_uid(power_req.the_chosen_one());
            notify_msg.set_conversation_id(power_req.conversation_id());
            notify_msg.set_power(power_req.power());
            notify.payload = notify_msg.SerializeAsString();
            broadcastNotify({power_req.the_chosen_one()}, notify);
        }
    public:
        WsHandlers(net::io_context& ioc
            ,WsRouter& router
            ,WsConnectionManager& manager
            ,std::shared_ptr<ServiceManager> services)
        :_ioc(ioc),_router(router),_manager(manager),_services(services)
        {
            router.Register(WS_AUTH, std::bind(&WsHandlers::Auth, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_SEND_MESSAGE, std::bind(&WsHandlers::SendMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_RECALL_MESSAGE, std::bind(&WsHandlers::RecallMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_TYPING, std::bind(&WsHandlers::Typing, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_FRIEND_REQUEST, std::bind(&WsHandlers::FriendRequest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_FRIEND_REQUEST_STATUS, std::bind(&WsHandlers::FriendRequestStatus, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_CREATE_CONVERSATION, std::bind(&WsHandlers::CreateConversation, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_REMOVE_CONVERSATION, std::bind(&WsHandlers::RemoveConversation, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_ADD_MEMBER, std::bind(&WsHandlers::AddMember, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_EXIT_CONVERSATION, std::bind(&WsHandlers::ExitConversation, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            router.Register(WS_CHANGE_POWER, std::bind(&WsHandlers::ChangePower, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        }
    private:
        net::io_context& _ioc;
        WsRouter& _router;
        WsConnectionManager& _manager;
        std::shared_ptr<ServiceManager> _services;
        std::shared_ptr<redis::RedisClient> _redis;
        std::unordered_map<std::string, std::string> _sessions;
        std::mutex _session_mutex;
    };
}
