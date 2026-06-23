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
        Response addChannel(const std::string& host)
        {
            Response rep;
            std::lock_guard<std::mutex> lock(_mutex);
            auto channel = std::make_shared<brpc::Channel>();
            brpc::ChannelOptions options;
            options.protocol = "baidu_std";
            options.timeout_ms = 3000;
            options.connect_timeout_ms = 1000;
            options.max_retry = 3;
            if(channel->Init(host.c_str(),&options) != 0)
            {
                rep.status = false;
                rep.errmsg = "初始化管道失败:" + host;
                return rep;
            }
            _channels[host] = channel;
            _cur = _channels.begin();
            return rep;
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
        Response chooseChannel(ChannelPtr* ptr)
        {
            Response rep;
            std::lock_guard<std::mutex> lock(_mutex);
            if(_channels.empty())
            {
                rep.status = false;
                rep.status = "没有可用的管道!";
                return rep;
            }
            _cur++;
            if(_cur == _channels.end())
                _cur = _channels.begin();
            *ptr = _cur->second;
            return rep;
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
        Response chooseService(const std::string& service_name,ServiceChannel::ChannelPtr* ptr)
        {
            Response rep;
            std::lock_guard<std::mutex> lock(_mutex);
            if(_active_services.find(service_name) == _active_services.end())
            {
                rep.status = false;
                rep.errmsg = "当前服务不活跃:" + service_name;
                return rep;
            }
            auto service = _services.find(service_name);
            if(service == _services.end())
            {
                rep.status = false;
                rep.errmsg = "不存在该服务:" + service_name;
                return rep;
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
        Response addService(const std::string& service_name,const std::string& host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_services.find(service_name) == _services.end())
            {
                _services[service_name] = std::make_shared<ServiceChannel>(service_name);
            }
            return _services[service_name]->addChannel(host);
        }
        Response removeService(const std::string& service_name,const std::string& host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_services.find(service_name) != _services.end())
            {
                _services[service_name]->removeChannel(host);
            }
            return Response();
        }
    private:
        std::mutex _mutex;
        std::unordered_set<std::string> _active_services;
        std::unordered_map<std::string, ServicePtr> _services;
    };
}