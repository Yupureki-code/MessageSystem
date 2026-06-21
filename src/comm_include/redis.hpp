#pragma once
#include "logger.hpp"
#include <chrono>
#include <iostream>
#include <memory>
#include <sw/redis++/redis++.h>
#include <string>
#include <sw/redis++/utils.h>
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
                opts.host = ip;
                opts.port = port;
                
                ConnectionPoolOptions pool_opts;
                pool_opts.size = thread_size;
                pool_opts.wait_timeout = std::chrono::minutes(late_time);
                _redis = std::make_unique<Redis>(opts,pool_opts);
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
        Response setex(const std::string& key,const std::string& value,int time = -1)
        {
            Response rep;
            std::string full = _business + ":" + _env + ":" + _version + ":" + key;
            Timer t(_monitor,"setex " + key + ": " +value);
            try
            {
                _redis->setex(full,time,value);
                rep.status = true;
            }
            catch(const Error& e)
            {
                LOG_ERROR("Redis setex失败:{}!",e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        Response get(const std::string& key,OptionalString* value)
        {
            Response rep;
            std::string full = _business + ":" + _env + ":" + _version + ":" + key;
            Timer t(_monitor,"get " + key);
            try
            {
                *value = _redis->get(full);
            }
            catch(const Error& e)
            {
                LOG_ERROR("Redis get失败:{}!",e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
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
        Response hset(const std::string& hash,const std::string& key,const std::string& value)
        {
            Response rep;
            std::string full = _business + ":" + _env + ":" + _version + ":" + hash;
            Timer t(_monitor,"set " + key + ":" + value);
            try
            {
                auto ret = _redis->hset(hash,key,value);
                if(!ret)
                {
                    rep.status = false;
                    rep.errmsg = "key不存在";
                    return rep;
                }
                rep.status = true;
            }
            catch(const Error& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        Response hincrby(const std::string& hash,const std::string& value,int x)
        {
            Response rep;
            std::string full = _business + ":" + _env + ":" + _version + ":" + hash;
            Timer t(_monitor,"hincrby " + hash + ":value" + ": " + std::to_string(x));
            try
            {
                auto ret = _redis->hincrby(hash, value, x);
                rep.value = ret;
                rep.status = true;
            }
            catch(const Error& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        Response hget(const std::string& hash,const std::string& key,OptionalString* value)
        {
            Response rep;
            std::string full = _business + ":" + _env + ":" + _version + ":" + hash;
            Timer t(_monitor,"hget " + full);
            try
            {
                *value = _redis->hget(hash,key);
            }
            catch(const Error& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        Response remove(const std::string& key)
        {
            Response rep;
            std::string full = _business + ":" + _env + ":" + _version + ":" + key;
            Timer t(_monitor,"remove " + key);
            try
            {
                _redis->del(full);
                rep.status = true;
            }
            catch(const Error& e)
            {
                LOG_ERROR("Redis remove失败:{}!",e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
    private:
        std::unique_ptr<Redis> _redis;
        latecyMonitor::LatencyMonitor _monitor;
        std::string _business;
        std::string _env;
        std::string _version;
    };
}
