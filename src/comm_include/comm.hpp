#pragma once
#include "logger.hpp"
#include <iostream>
#include "config.h"
#include <iomanip>

namespace messageSystem
{
    const std::string FILE_SERVICE = "file_service";
    const std::string USER_SERVICE = "user_service";
    const std::string AMQP_MESSAGE_EXCHANGE = "amqp_message_exchange";
    const std::string AMQP_MESSAGE_POST_QUEUE = "amqp_messages_post_queue";
    const std::string AMQP_MESSAGE_DELETE_QUEUE = "amqp_messages_delete_queue";
    const std::string AMQP_MESSAGE_POST_ROUTING_KEY = "amqp_messages_post_routing_key";
    struct Response 
    {
        bool status;         // 0 表示成功，负值表示错误
        std::string errmsg; // 错误信息
        
        Response() : status(true) {}
        Response(int s, const std::string& msg = "") : status(s), errmsg(msg) {}
    };
};

namespace util
{
    class StringUtil
    {
    public:
        //从字符串中根据分隔符，获得每部分的字符串
        static void SplitString(std::string str,std::string space,std::vector<std::string>* v)
        {
            size_t pos = 0;
            while ((pos = str.find(space)) != std::string::npos)
            {
                std::string token = str.substr(0, pos);
                if (!token.empty())
                {
                    v->push_back(token);
                }
                str.erase(0, pos + space.length());
            }
            if (!str.empty())
            {
                v->push_back(str);
            }
        }
        //移除空格，换行符等
        static std::string RemoveAllWhitespace(const std::string& str)
        {
            std::string result;
            for (char c : str)
            {
                if (c != ' ' && c != '\n' && c != '\r' && c != '\t')
                {
                    result += c;
                }
            }
            return result;
        }
        //去除字符串首尾的空格
        static std::string Trim(const std::string& s)
        {
            size_t start = 0;
            while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start])))
                ++start;
            size_t end = s.size();
            while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1])))
                --end;
            return s.substr(start, end - start);
        }
        static std::string generateUniqueName() 
        {
            static std::atomic<uint64_t> counter{0};
            auto now = std::chrono::system_clock::now();
            auto time_t = std::chrono::system_clock::to_time_t(now);
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                now.time_since_epoch()) % 1000000;
            
            std::tm tm = *std::localtime(&time_t);
            std::ostringstream oss;
            oss << std::put_time(&tm, "%Y%m%d%H%M%S") 
                << "-" << std::setfill('0') << std::setw(6) << us.count()
                << "-" << counter.fetch_add(1);
            return oss.str();
        }

    };
    class TimeUtil
    {
    public:
        // 获取当前时间戳（秒级，int64_t）
        static int64_t getCurrentTimestampSeconds() {
            // 明确类型：std::chrono::time_point<std::chrono::system_clock>
            std::chrono::time_point<std::chrono::system_clock> now_point = 
                std::chrono::system_clock::now();
            
            // 转换为自纪元以来的秒数
            std::chrono::seconds seconds_since_epoch = 
                std::chrono::duration_cast<std::chrono::seconds>(
                    now_point.time_since_epoch()
                );
            
            // 返回int64_t类型的秒数
            return seconds_since_epoch.count();
        }
        
        // 计算存储的时间戳与当前时间的秒级时间差
        static int getSecondsSinceTimestamp(int64_t stored_timestamp) {
            // 获取当前时间戳
            int64_t current_timestamp = getCurrentTimestampSeconds();
            
            // 计算时间差（确保在int范围内）
            int64_t diff_seconds = current_timestamp - stored_timestamp;
            
            // 转换为int（假设时间差在int范围内）
            return static_cast<int>(diff_seconds);
        }
        
    };
}
