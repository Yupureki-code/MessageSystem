#pragma once
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <etcd/KeepAlive.hpp>
#include <functional>
#include <memory>
#include "comm.hpp"
#include <etcd/Watcher.hpp>

namespace messageSystem
{
    class Registrant
    {
    public:
        Registrant(const std::string& host)
        :_client(host),_keep_alive(_client.leasekeepalive(3).get())
        {
        }
        bool Register(const std::string& key,const std::string& value)
        {
            auto rep = _client.put(key,value).get();
            if(!rep.is_ok())
            {
                LOG_ERROR("注册数据失败:{}!",rep.error_message());
                return false;
            }
            return true;
        }
    private:
        etcd::Client _client;
        std::shared_ptr<etcd::KeepAlive> _keep_alive;
    };
    class Discovery
    {
    private:
        void callBack(const etcd::Response& rep)
        {
            if(!rep.is_ok())
            {
                std::cout<<rep.error_message()<<std::endl;
            }
            else
            {
                for(auto & ev : rep.events())
                {
                    if(ev.event_type() == etcd::Event::EventType::PUT)
                    {
                        _put_cb(ev.kv().key(),ev.kv().as_string());
                    }
                    else if(ev.event_type() == etcd::Event::EventType::DELETE_)
                    {
                        _del_cb(ev.kv().key(),ev.kv().as_string());
                    }
                }
            }
        }
    public:
        using NotifyCallback = std::function<void(std::string, std::string)>;
        Discovery(const std::string& host,const std::string& basedir,NotifyCallback put,NotifyCallback del)
        :_put_cb(put),_del_cb(del),_client(host),_watcher(host,basedir,std::bind(&Discovery::callBack,this,std::placeholders::_1),true)
        {
            auto rep = _client.ls(basedir).get();
            if(!rep.is_ok())
            {
                LOG_ERROR("获取数据失败:{}!",rep.error_message());
                return;
            }
            for(size_t i = 0;i<rep.keys().size();i++)
            {
                _put_cb(rep.key(i),rep.value(i).as_string());
            }
        }
    private:
        NotifyCallback _put_cb;
        NotifyCallback _del_cb;
        etcd::Client _client;
        etcd::Watcher _watcher;
    };
}