#pragma once
#include "comm.hpp"
#include <brpc/channel.h>
#include <brpc/server.h>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace messageSystem
{
    
    class ServiceChannel
    {
    public:
        using ChannelPtr = std::shared_ptr<brpc::Channel>;
        ServiceChannel(const std::string& service_name)
        :_service_name(service_name),_cur(_channels.begin())
        {}
        bool addChannel(const std::string& host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto channel = std::make_shared<brpc::Channel>();
            brpc::ChannelOptions options;
            options.protocol = "baidu_std";
            options.timeout_ms = -1;
            options.connect_timeout_ms = -1;
            options.max_retry = 3;
            if(channel->Init(host.c_str(),&options) != 0)
            {
                LOG_ERROR("初始化管道失败:{}!",host);
                return false;
            }
            _channels[host] = channel;
            _cur = _channels.begin();
            return true;
        }
        void removeChannel(const std::string& host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto channel = _channels.find(host);
            if(channel == _channels.end())
                return;
            _channels.erase(channel);
            _cur = _channels.begin();
        }
        bool chooseChannel(ChannelPtr* ptr)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_channels.empty())
            {
                LOG_ERROR("没有可用的管道!");
                return false;
            }
            _cur++;
            if(_cur == _channels.end())
                _cur = _channels.begin();
            *ptr = _cur->second;
            return true;
        }
    private:
        std::mutex _mutex;
        std::string _service_name;
        std::unordered_map<std::string, ChannelPtr> _channels;
        std::unordered_map<std::string, ChannelPtr>::iterator _cur;
    };

    class ServiceManager
    {
    public:
        using ServicePtr = std::shared_ptr<ServiceChannel>;
        bool chooseService(const std::string& service_name,ServiceChannel::ChannelPtr* ptr)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_active_services.find(service_name) == _active_services.end())
            {
                LOG_DEBUG("当前服务不活跃:{}!",service_name);
                return false;
            }
            auto service = _services.find(service_name);
            if(service == _services.end())
            {
                LOG_ERROR("不存在该服务:{}!",service_name);
                return false;
            }
            return service->second->chooseChannel(ptr);
        }
        void activeService(const std::string& service_name)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _active_services.insert(service_name);
        }
        void inactiveService(const std::string& service_name)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto service = _active_services.find(service_name);
            if(service == _active_services.end())
            {
                LOG_DEBUG("该服务已失活:{}!",service_name);
                return;
            }
            _active_services.erase(service);
        }
        bool addService(const std::string& service_name,const std::string& host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_services.find(service_name) == _services.end())
            {
                _services[service_name] = std::make_shared<ServiceChannel>(service_name);
            }
            return _services[service_name]->addChannel(host);
        }
        bool removeService(const std::string& service_name,const std::string& host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_services.find(service_name) != _services.end())
            {
                _services[service_name]->removeChannel(host);
            }
            return true;
        }
    private:
        std::mutex _mutex;
        std::unordered_set<std::string> _active_services;
        std::unordered_map<std::string, ServicePtr> _services;
    };
}