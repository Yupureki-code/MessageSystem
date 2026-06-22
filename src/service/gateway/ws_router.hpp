#pragma once
#include "ws_protocol.hpp"
#include "ws_connection.hpp"
#include <boost/asio/awaitable.hpp>
#include <functional>
#include <unordered_map>
#include <string>

namespace messageSystem
{
    using CoroutineHandler = std::function<
        boost::asio::awaitable<void>(
            const std::string& conn_id,
            const WsProtocolReq& msg,
            std::shared_ptr<Connection> conn
        )
    >;
    class WsRouter
    {
    public:
        void Register(const std::string& type,CoroutineHandler handler)
        {
            _handlers[type] = std::move(handler);
        }
        boost::asio::awaitable<void> dispatch(
        const std::string& conn_id,
        WsProtocolReq& req,
        std::shared_ptr<Connection> conn)
        {
            auto it = _handlers.find(req.type);
            if (it == _handlers.end()) 
            {
                WsProtocolRsp rep;
                rep.error = "未知消息类型: " + req.type;
                conn->ws->write(boost::asio::buffer(req.serialize()));
                co_return;
            }
            co_await it->second(conn_id, req, conn);
        }
    private:
        std::unordered_map<std::string, CoroutineHandler> _handlers;
    };
}