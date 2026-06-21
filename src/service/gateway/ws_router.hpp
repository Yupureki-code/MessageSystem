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
            const WsProtocol& msg,
            std::shared_ptr<Connection> conn
        )
    >;
    class WsRouter
    {
    public:
        void registerHandler(const std::string& type,CoroutineHandler handler)
        {
            _handlers[type] = std::move(handler);
        }
        boost::asio::awaitable<void> dispatch(
        const std::string& conn_id,
        WsProtocol& msg,
        std::shared_ptr<Connection> conn)
        {
            auto it = _handlers.find(msg.type);
            if (it == _handlers.end()) 
            {
                msg.error = "未知消息类型: " + msg.type;
                conn->ws->write(boost::asio::buffer(msg.serialize()));
                co_return;
            }
            co_await it->second(conn_id, msg, conn);
        }
    private:
        std::unordered_map<std::string, CoroutineHandler> _handlers;
    };
}