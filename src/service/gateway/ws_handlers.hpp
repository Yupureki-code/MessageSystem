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
        // --- 正常 / 标准码 ---
        NormalClosure       = 1000,  // 正常关闭：连接已完成其通信目的
        GoingAway           = 1001,  // 终端离开：如服务器关机或浏览器页面跳转
        ProtocolError       = 1002,  // 协议错误：收到了不符合协议规范的帧
        UnsupportedData     = 1003,  // 不支持的数据类型：例如只接受文本却收到了二进制
        // 1004 保留，无定义
        NoStatusReceived    = 1005,  // 【仅内部使用】表示未收到状态码，不可主动发送
        AbnormalClosure     = 1006,  // 【仅内部使用】连接异常断开（无Close帧），不可主动发送
        InvalidPayloadData  = 1007,  // 帧负载数据与消息类型不一致（如文本中包含非UTF-8）
        PolicyViolation     = 1008,  // 策略违规：通用策略错误码（限流、鉴权等）
        MessageTooBig       = 1009,  // 消息过大，超出了接收端的处理能力
        MissingExtension    = 1010,  // 客户端期望服务器协商某个扩展，但服务器未返回
        InternalError       = 1011,  // 服务器内部错误：遇到意外情况无法完成请求
        ServiceRestart      = 1012,  // 服务器正在重启
        TryAgainLater       = 1013,  // 服务器临时过载，建议稍后重试
        // 1014 保留（Bad Gateway，供WebSocket扩展保留）
        TLSHandshakeFailure = 1015,  // 【仅内部使用】TLS握手失败，不可主动发送
        // --- 私有用途段 (4000-4999) 示例，应用可自行扩展 ---
        // 以下是一些常见应用自定义码的示例
        Unauthorized        = 4001,  // 未授权 / 会话过期
        DuplicateConnection = 4002,  // 重复连接
        RateLimitExceeded   = 4003,  // 超出频率限制
    };
    class WsHandlers
    {
    private:
        bool CheckSessionID(const WsProtocol& msg)
        {
            if(_sessions[msg.user_id] != msg.session_id)
                return false;
            return true;
        }
        void HandlerError(WsProtocol& msg,WsCloseCode status_code)
        {
            int code = static_cast<int>(status_code);
            msg.code = code;
            switch (code) 
            {
                case 1001:
                    msg.error = "用户已下线!";
                    break;
            }
        }
        boost::asio::awaitable<void> Auth(
            const std::string& conn_id,
            const WsProtocol& msg,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocol response;
            nlohmann::json json = nlohmann::json::parse(msg.payload);
            std::string uid = msg.user_id;
            std::string token = json.value("token","");
            sw::redis::OptionalString get_token;
            auto rep = _redis->get("token:" + uid, &get_token);
            if(!rep.status || !get_token || get_token.value() != token)
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await conn->ws->async_write(net::buffer(response.serialize()));
                co_return;
            }
            response.code = 1000;
            response.payload = "登陆成功!";
            std::string session_id = util::StringUtil::generateUniqueName();
            _sessions[uid] = session_id;
            response.session_id = session_id;
            co_await conn->ws->async_write(net::buffer(response.serialize()));
        }
        boost::asio::awaitable<void> SendMessage(
            const std::string& conn_id,
            const WsProtocol& msg,
            std::shared_ptr<Connection> conn
        )
        {
            WsProtocol response;
            if(!CheckSessionID(msg))
            {
                HandlerError(response, WsCloseCode::Unauthorized);
                co_await conn->ws->async_write(net::buffer(response.serialize()));
                co_return;
            }
            ServiceChannel::ChannelPtr channel;
            auto rep = _services->chooseService(CONVERSATION_SERVICE, &channel);
            SendMessageReq req;
            SendMessageRsq rsp;
            if(!req.ParseFromString(msg.payload))
            {
                HandlerError(response, WsCloseCode::ProtocolError);
                co_await conn->ws->async_write(net::buffer(response.serialize()));
                co_return;
            }
            co_await runInLoop(nullptr, _ioc, [channel,req,&rsp](::google::protobuf::Closure *done){
                MessageServer_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.SendMessage(&cntl, &req, &rsp, done);
            });
            response.code = 1000;
            response.type = WS_NEW_MESSAGE;
            response.payload = req.message().SerializeAsString();
            std::string rep_str = response.serialize();
            for(auto & it : *rsp.mutable_uids())
            {
                auto conn = _manager.get(it);
                if(!conn)
                    continue;
                std::string out;
                co_await conn->ws->async_write(net::buffer(rep_str),net::use_awaitable);
            }
            response.type = WS_SEND_MESSAGE;
            response.payload = "发送成功!";
            co_await conn->ws->async_write(net::buffer(rep_str),net::use_awaitable); 
        }
    public:
        WsHandlers(net::io_context& ioc
            ,WsRouter& _router
            ,WsConnectionManager& manager
            ,std::shared_ptr<ServiceManager> services)
        :_ioc(ioc),_router(_router),_manager(manager),_services(services)
        {

        }
    private:
        net::io_context& _ioc;
        WsRouter& _router;
        WsConnectionManager& _manager;
        std::shared_ptr<ServiceManager> _services;
        std::shared_ptr<redis::RedisClient> _redis;
        std::unordered_map<std::string, std::string> _sessions;
    };
};