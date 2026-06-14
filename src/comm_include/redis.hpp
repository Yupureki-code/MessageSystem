#include "logger.hpp"
#include <chrono>
#include <iostream>
#include <memory>
#include <sw/redis++/redis++.h>
#include <string>
#include "latecymonitor.hpp"
#include "comm.hpp"

namespace redis
{
    using namespace sw::redis;
    using namespace messageSystem;
    using latecyMonitor::Timer;
    class RedisClient
    {
    public:
        RedisClient(const std::string& ip = "127.0.0.1",int port = 6379,int thread_size = 10,int late_time = 5)
        :_business("messageSystem"),_env("prod"),_version("v1")
        {
            try
            {
                ConnectionOptions opts;
                opts.host = ip;  // Redis 服务器地址
                opts.port = port;         // Redis 端口
                // opts.password = "your_password";  // 如果有密码
                // opts.db = 0;  // 数据库索引，默认为0
                
                // 创建连接池选项（线程安全）
                ConnectionPoolOptions pool_opts;
                pool_opts.size = thread_size;  // 连接池大小
                pool_opts.wait_timeout = std::chrono::minutes(late_time);
                // 创建 Redis 客户端（带连接池）
                _redis = std::make_unique<Redis>(opts,pool_opts);
                // 测试连接
                std::string ping_result = _redis->ping();
                LOG_DEBUG("Redis Ping:",ping_result);
            }
            catch (const Error &e) 
            {
                LOG_ERROR("Redis连接错误:{}!",e.what());
                return;
            }
            _monitor.setOutputFile(LOG_PATH, "odb_user.log");
            _monitor.start();
        }
        void setPrefix(const std::string& business,const std::string& env,const std::string& version)
        {
            _business = business;
            _env = env;
            _version = version;
        }
        void setex(const std::string& key,const std::string& value,int time = -1)
        {
            std::string full = _business + ":" + _env + ":" + _version + ":" + key;
            Timer t(_monitor,"setex " + key + ": " +value);
            _redis->setex(full,time,value);
        }
        bool get(const std::string& key,std::string* value)
        {
            std::string full = _business + ":" + _env + ":" + _version + ":" + key;
            Timer t(_monitor,"get " + key);
            auto ret = _redis->get(full);
            if(!ret)
                return false;
            *value = *ret;
            return true;
        }
        bool exists(const std::string& key)
        {
            std::string full = _business + ":" + _env + ":" + _version + ":" + key;
            Timer t(_monitor,"exists " + key);
            auto ret = _redis->exists(full);
            if(!ret)
                return false;
            return true;
        }
        void remove(const std::string& key)
        {
            std::string full = _business + ":" + _env + ":" + _version + ":" + key;
            Timer t(_monitor,"exists " + key);
            _redis->del(key);
        }
    private:
        std::unique_ptr<Redis> _redis;
        latecyMonitor::LatencyMonitor _monitor;
        std::string _business;
        std::string _env;
        std::string _version;
    };
};