#pragma once
#include <amqpcpp.h>
#include <amqpcpp/address.h>
#include <amqpcpp/channel.h>
#include <amqpcpp/connection.h>
#include <amqpcpp/flags.h>
#include <amqpcpp/libev.h>
#include <amqpcpp/linux_tcp/tcpconnection.h>
#include <amqpcpp/message.h>
#include <ev.h>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include "comm.hpp"

namespace messageSystem
{
    class RabbitMQ
    {
    private:
        static void watcher_callback(struct ev_loop *loop, ev_async *watcher, int32_t revents) 
        {
            ev_break(loop, EVBREAK_ALL);
        }
    public:
        using MessageCallback = std::function<void(const char*, size_t)>;
        RabbitMQ(const std::string& user,const std::string& password
            ,const std::string& host)
        {
            _loop = ev_default_loop(0);
            _handler = std::make_unique<AMQP::LibEvHandler>(_loop);
            std::string url = "amqp://" + user + ":" + password + "@" + host + "/";
            AMQP::Address addr(url.c_str());
            _connection = std::make_unique<AMQP::TcpConnection>(_handler.get(),addr);
            _channel = std::make_unique<AMQP::TcpChannel>(_connection.get());
            _loop_thread = std::thread([this](){
                ev_run(_loop,0);
            });
        }
        ~RabbitMQ()
        {
            ev_async async_watcher;
            ev_async_init(&async_watcher, watcher_callback);
            ev_async_start(_loop, &async_watcher);
            ev_async_send(_loop, &async_watcher);
            _loop_thread.join();
            _channel.reset();
            _connection.reset();
            _handler.reset();
        }
        void addExchange(const std::string& name,AMQP::ExchangeType type = AMQP::direct,int flags = 0)
        {
            _channel->declareExchange(name,type,flags)
            .onError([name](const char* message)
            {
                LOG_ERROR("声明交换机失败:{}-{}!",name,message);
            })
            .onSuccess([name,this](){
                LOG_INFO("声明交换机成功:{}!",name);
                std::lock_guard<std::mutex> lock(_mutex);
                _exchanges.insert(name);
            });
        }
        void addQueue(const std::string &name, int flags = AMQP::durable)
        {
            _channel->declareQueue(name,flags)
            .onError([name](const char* message)
            {
                LOG_ERROR("声明队列失败:{}-{}!",name,message);
            })
            .onSuccess([name,this](){
                LOG_INFO("声明队列成功:{}!",name);
                std::lock_guard<std::mutex> lock(_mutex);
                _queues.insert(name);
            });
        }
        bool bind(const std::string &exchange,const std::string& queue,const std::string& key)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_exchanges.find(exchange) == _exchanges.end())
            {
                LOG_ERROR("未找到该交换机:{}!",exchange);
                return false;
            }
            if(_queues.find(queue) == _queues.end())
            {
                LOG_ERROR("未找到该队列:{}!",queue);
                return false;
            }
            _channel->bindQueue(exchange,queue,key)
            .onError([exchange,queue](const char* message)
            {
                LOG_ERROR("绑定失败:{}-{}-{}!",exchange,queue,message);
            })
            .onSuccess([exchange,queue](){
                LOG_INFO("绑定成功:{}-{}!",exchange,queue);
            });
            return true;
        }
        bool publish(const std::string &exchange, const std::string &msg, const std::string &routing_key) 
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_exchanges.find(exchange) == _exchanges.end())
            {
                LOG_ERROR("未找到该交换机:{}!",exchange);
                return false;
            }
            LOG_DEBUG("向交换机 {}-{} 发布消息！", exchange, routing_key);
            bool ret = _channel->publish(exchange, routing_key, msg);
            if (ret == false) 
            {
                LOG_ERROR("{} 发布消息失败！", exchange);
                return false;
            }
            return true;
        }
        bool consume(const std::string &queue, const MessageCallback &cb) 
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_queues.find(queue) == _queues.end())
            {
                LOG_ERROR("未找到该队列:{}!",queue);
                return false;
            }
            LOG_DEBUG("开始订阅 {} 队列消息！", queue);
            _channel->consume(queue, "consume-tag") 
            .onReceived([this, cb](const AMQP::Message &message, 
                uint64_t deliveryTag, 
                bool redelivered) {
                cb(message.body(), message.bodySize());
                _channel->ack(deliveryTag);
            })
            .onError([queue](const char *message){
                LOG_ERROR("订阅 {} 队列消息失败: {}", queue, message);
            });
            return true;
        }
    private:
        struct ev_loop *_loop;
        std::unique_ptr<AMQP::LibEvHandler> _handler;
        std::unique_ptr<AMQP::TcpConnection> _connection;
        std::unique_ptr<AMQP::TcpChannel> _channel;
        std::unordered_set<std::string> _exchanges;
        std::unordered_set<std::string> _queues;
        std::mutex _mutex;
        std::thread _loop_thread;
    };
}
