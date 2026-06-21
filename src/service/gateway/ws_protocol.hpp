#pragma once
#include <string>
#include <nlohmann/json.hpp>

namespace messageSystem
{
    struct WsProtocol
    {
        using json = nlohmann::json;
        std::string type;
        std::string payload;
        std::string session_id;
        std::string user_id;
        int code = 0;
        std::string error;
        // 序列化: WsProtocol → JSON 字符串
        std::string serialize() const 
        {
            nlohmann::json j;
            j["type"] = type;
            j["session_id"] = session_id;
            j["user_id"] = user_id;
            if (!payload.empty()) j["payload"] = payload;
            if (code != 0) j["code"] = code;
            if (!error.empty()) j["error"] = error;
            return j.dump();
        }
        // 反序列化: JSON 字符串 → WsProtocol
        void deserialize(const std::string& raw) 
        {
            auto j = nlohmann::json::parse(raw);
            type    = j.value("type", "");
            payload = j.value("payload", "");
            session_id = j.value("session_id","");
            user_id = j.value("user_id","");
            code    = j.value("code", 0);
            error   = j.value("error", "");
        }
    };
};