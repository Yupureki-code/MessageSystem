#pragma once
#include <boost/beast.hpp>
#include <boost/asio.hpp>
#include <unordered_map>
#include <string>
#include <memory>
#include <mutex>
#include "../../comm_include/comm.hpp"

namespace messageSystem
{
    namespace beast = boost::beast;
    namespace net   = boost::asio;
    using tcp       = boost::asio::ip::tcp;
    // WebSocket 流类型
    using websocket_t = beast::websocket::stream<tcp::socket>;
    // 连接信息
    struct Connection 
    {
        std::string user_id;
        std::string session_id;
        std::string token;
        std::shared_ptr<websocket_t> ws;
        bool authenticated = false;
    };
    using ConnectionPtr = std::shared_ptr<Connection>;
    class WsConnectionManager
    {
    public:
        std::string add(std::shared_ptr<websocket_t> ws)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            std::string session_id = util::StringUtil::generateUniqueName();
            ConnectionPtr ptr = std::make_shared<Connection>();
            ptr->session_id = session_id;
            ptr->ws = std::move(ws);
            _connections[session_id] = std::move(ptr);
            return session_id;
        }
        void authenticate(const std::string& session_id,const std::string& user_id)
        {
            auto it = _connections.find(session_id);
            if(it == _connections.end())
                return;
            it->second->user_id = user_id;
            it->second->authenticated = true;
            _authenticated_users[user_id] = session_id;
        }
        void remove(const std::string& session_id)
        {
            auto it = _connections.find(session_id);
            if(it == _connections.end())
                return;
            _authenticated_users.erase(it->second->user_id);
            _connections.erase(it);
        }
        ConnectionPtr get(const std::string& session_id)
        {
            auto it = _connections.find(session_id);
            if(it == _connections.end())
                return nullptr;
            return it->second;
        }
        ConnectionPtr getByUid(const std::string& user_id)
        {
            auto it = _authenticated_users.find(user_id);
            if(it == _authenticated_users.end())
                return nullptr;
            return get(it->second);
        }
        int onlineCount()
        {
            return _connections.size();
        }
    private:
        std::mutex _mutex;
        std::unordered_map<std::string, ConnectionPtr> _connections;
        std::unordered_map<std::string, std::string> _authenticated_users;
    };
}