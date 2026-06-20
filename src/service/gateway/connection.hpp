#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include "../../comm_include/comm.hpp"

namespace messageSystem
{
    struct ConnectionInfo 
    {
        std::string user_id;                    // 用户 ID (认证后设置)
        std::string session_id;                 // 会话 ID
        std::string token;                      // 认证 token
        std::string device_id;                  // 设备 ID
        std::string client_ip;                  // 客户端 IP
        int64_t connect_time;                   // 连接建立时间
        int64_t last_heartbeat;                 // 最后心跳时间
        bool authenticated;                     // 是否已认证
        ConnectionInfo()
            : connect_time(util::TimeUtil::getCurrentTimestampSeconds())
            , last_heartbeat(util::TimeUtil::getCurrentTimestampSeconds())
            , authenticated(false)
        {}
    };
    class ConnectionManager
    {
    private:
        using server_t = websocketpp::server<websocketpp::config::asio>;
        using message_ptr = server_t::message_ptr;
        using connection_hdl = websocketpp::connection_hdl;
        using ConnectionPtr = std::shared_ptr<ConnectionInfo>;
        void onOpen(connection_hdl hdl) 
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto con = _server.get_con_from_hdl(hdl);
            ConnectionPtr connection = std::make_shared<ConnectionInfo>();
            std::string host = con->get_remote_endpoint();
            connection->client_ip = host;
            _unauthenticated_connections[host] = connection;
            LOG_INFO("连接已建立(host):",host);
        }
        void onClose(connection_hdl hdl)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto con = _server.get_con_from_hdl(hdl);
            std::string host = con->get_remote_endpoint();
            if(_unauthenticated_connections.find(host) != _unauthenticated_connections.end())
            {
                _unauthenticated_connections.erase(host);
            }
            if(_authenticated_connections.find(host) != _authenticated_connections.end())
            {
                _authenticated_connections.erase(host);
            }
            LOG_INFO("连接已关闭(host):",host);
        }
        void onError(connection_hdl hdl)
        {
            auto con = _server.get_con_from_hdl(hdl);
            LOG_ERROR("连接异常(host):",con->get_remote_endpoint());
        }
        void onMessage(connection_hdl hdl,message_ptr message)
        {
            
        }
    public:
        ConnectionManager()
        {
            _server.set_access_channels(websocketpp::log::alevel::all);
            _server.clear_access_channels(websocketpp::log::alevel::frame_payload);
            _server.init_asio();
            _server.set_open_handler(std::bind(&ConnectionManager::onOpen,this,std::placeholders::_1));
            _server.set_close_handler(std::bind(&ConnectionManager::onClose,this,std::placeholders::_1));
            _server.set_fail_handler(std::bind(&ConnectionManager::onError,this,std::placeholders::_1));
            _server.set_message_handler(std::bind(&ConnectionManager::onMessage,this,std::placeholders::_1,std::placeholders::_2));
        }
    private:
        server_t _server;
        std::mutex _mutex;
        std::unordered_map<std::string, ConnectionPtr> _unauthenticated_connections;
        std::unordered_map<std::string, ConnectionPtr> _authenticated_connections;
    };
};