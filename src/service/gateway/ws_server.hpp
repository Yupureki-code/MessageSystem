#pragma once
#include "ws_router.hpp"
#include "ws_connection.hpp"
#include <boost/beast.hpp>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/signal_set.hpp>
#include <thread>

namespace messageSystem
{
    namespace beast = boost::beast;
    namespace net   = boost::asio;
    using tcp       = boost::asio::ip::tcp;
    class WsServer
    {
    private:
        boost::asio::awaitable<void> sessionLoop(websocket_t ws)
        {
            // ① 注册连接
            std::string conn_id = _manager.add(std::make_shared<websocket_t>(std::move(ws)));
            auto conn = _manager.get(conn_id);
            if(!conn)
                co_return;
            try 
            {
                // 设置读取超时 (30 秒无数据则断开)
                // 注意: 需要在 async_read 之前设置
                conn->ws->set_option(
                    beast::websocket::stream_base::timeout::suggested(beast::role_type::server));
                // ② 消息循环
                beast::flat_buffer buffer;
                for(;;)
                {
                    // co_await 挂起协程，等待数据到达
                    // 数据到达后协程恢复，buffer 中是完整的消息
                    co_await conn->ws->async_read(buffer, net::use_awaitable);
                    // 解析消息
                    std::string raw = beast::buffers_to_string(buffer.data());
                    buffer.consume(buffer.size());  // 清空 buffer
                    WsProtocolReq req;
                    WsProtocolRsp rep;
                    try 
                    {
                        req.deserialize(raw);
                    } 
                    catch (const std::exception& e) 
                    {
                        rep.error = "请求解析错误!";
                        break;
                    }
                    if (req.type.empty()) 
                    {
                        rep.error = "缺少 type 字段";
                        co_await conn->ws->async_write(net::buffer(rep.serialize()), net::use_awaitable);
                        continue;
                    }
                    // ③ 路由到 Handler (co_await 等待 Handler 执行完毕)
                    co_await _router.dispatch(conn_id, req, conn);
                }
            } 
            catch (const beast::system_error& e) 
            {
                // WebSocket 错误 (正常断开、超时等)
                if (e.code() == beast::websocket::error::closed) 
                {
                    // 客户端正常断开
                } 
                else 
                {
                    // 其他错误
                }
            } catch (const std::exception& e) 
            {
                // 其他异常
            }
            // ④ 清理连接
            _manager.remove(conn_id);
        }
        boost::asio::awaitable<void> listener(tcp::endpoint endpoint)
        {   
            // 创建 TCP acceptor
            tcp::acceptor acceptor(_ioc, endpoint);
            acceptor.set_option(net::socket_base::reuse_address(true));
            for(;;)
            {
                // co_await 等待新连接
                // 有新连接到达时协程恢复
                tcp::socket socket = co_await acceptor.async_accept(net::use_awaitable);
                // 创建 WebSocket 流
                auto ws = std::make_shared<websocket_t>(std::move(socket));
                // 设置 WebSocket 选项
                ws->set_option(beast::websocket::stream_base::timeout::suggested(
                    beast::role_type::server));
                ws->set_option(beast::websocket::stream_base::decorator(
                    [](beast::websocket::response_type& res) {
                        res.set(beast::http::field::server, "IM-WebSocket-Server");
                    }));
                // co_await WebSocket 握手
                try 
                {
                    co_await ws->async_accept(net::use_awaitable);
                } 
                catch (...) 
                {
                    continue;  // 握手失败，跳过
                }
                // 为每个连接 spawn 一个 session_loop 协程
                // detached 表示 "即发即忘"，不等待协程结束
                net::co_spawn(
                    _ioc,
                    sessionLoop(websocket_t(std::move(*ws))),
                    net::detached
                );
            }
        }
    public:
        WsServer(int port,int io_threads = 4)
        :_port(port),_io_threads(io_threads)
        {

        }
        void registerHandler(const std::string& type,CoroutineHandler handler)
        {
            _router.Register(type, handler);
        }
        void run() 
        {
            auto endpoint = tcp::endpoint(net::ip::make_address("0.0.0.0"), _port);
            // 启动 listener 协程
            net::co_spawn(
                _ioc,
                listener(endpoint),
                [](std::exception_ptr e) {
                    if (e) {
                        try { std::rethrow_exception(e); }
                        catch (const std::exception& ex) {
                        }
                    }
                }
            );
            // 启动 I/O 线程池
            for (int i = 0; i < _io_threads; ++i) 
            {
                std::thread([this](){_ioc.run();}).detach();
            }
        }
        void addSession()
        {
            
        }
    private:
        int _port;
        int _io_threads;
        net::io_context _ioc;
        WsRouter _router;
        WsConnectionManager _manager;
    };
}