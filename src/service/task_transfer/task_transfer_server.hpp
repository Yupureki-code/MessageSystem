#pragma once
#include "../../comm_include/rabbitmq.hpp"
#include "../../comm_include/comm.hpp"
#include "../../comm_include/proto_include/task_transfer.pb.h"
#include "../../comm_include/proto_include/message_store.pb.h"
#include "../../comm_include/channel.hpp"
#include "../../comm_include/etcd.hpp"
#include <amqpcpp/flags.h>
#include <any>
#include <brpc/backup_request_policy.h>
#include <brpc/callback.h>
#include <chrono>
#include <cstdint>
#include <fmt/core.h>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <utility>

namespace messageSystem
{

    struct TaskRelayConfig
    {
        int         prefetch_count    = 200;
        int         internal_queue_capacity = 5000;
        int         max_batch_size    = 100;
        std::chrono::milliseconds _flush_interval{10};
        std::chrono::seconds _retry_interval{1};
        int         brpc_timeout_ms   = 2000;
        int         brpc_max_retry    = 3;
    };
    struct TaskEntry
    {
        std::string payload_bytes;
        uint64_t deliveryTag;
    };

    class TaskTransferServer;
    struct RoutingInfo
    {
        std::string routing;
        std::any payload;
        std::string host;
        size_t nums = 0;
        bool in_flush_queue = false;
        int retry_count = 0;
        int retry_interval = 1;
        uint64_t last_retry_timestamp;
        std::queue<uint64_t> _deliveryTags;
        void clear(std::shared_ptr<RabbitMQ> mq,int _retry_interval,bool ack)
        {
            nums = 0;
            retry_count = 0;
            payload.reset();
            in_flush_queue = false;
            while(_deliveryTags.size())
            {
                if(ack && mq)
                    mq->ack(_deliveryTags.front());
                _deliveryTags.pop();
            }
            retry_interval = _retry_interval;
        }
        void updateRetryTime() 
        {
            last_retry_timestamp = util::TimeUtil::getCurrentTimestampSeconds();
        }
        bool canRetry()
        {
            return util::TimeUtil::getSecondsSinceTimestamp(last_retry_timestamp) >= retry_interval;
        }
    };
    class TaskTransferServer
    {
    private:
        void asyncCallback(brpc::Controller* cntl, CommRsp* response, RoutingInfo* routing_info)
        {
            {
                std::lock_guard<std::mutex> lock(*_mutex);
                auto& routing = routing_info->routing;
                if (cntl->Failed())
                {
                    LOG_ERROR("BRPC调用失败:{}!",cntl->ErrorText());
                    if(this_thread::get_id() == _retry_queue.getThreadId())
                    {
                        routing_info->retry_count++;
                        routing_info->retry_interval *=2;
                    }
                    _retry_queue.pushRetryRouting(*routing_info);
                    if(this_thread::get_id() != _retry_queue.getThreadId())
                        _routings[routing].clear(_mq,_config._retry_interval.count(),false);
                }
                else
                {
                    LOG_DEBUG("BRPC调用成功:routing={}", routing);
                    routing_info->clear(_mq,_config._retry_interval.count(),true);
                }
            }
            delete cntl;
            delete response;
        }
        /// @brief 解析并合并PostMessages请求
        /// @param body protobuf序列化的PostMessagesReq
        /// @return 本次合并的消息数
        size_t ParsePostMessages(const std::string& body)
        {
            messageSystem::PostMessagesReq payload;
            if(!payload.ParseFromString(body))
            {
                LOG_ERROR("解析PostMessagesReq失败!");
                return 0;
            }
            auto& routing = _routings[AMQP_MESSAGE_POST_ROUTING_KEY];
            if(!routing.payload.has_value())
            {
                routing.payload = std::make_shared<PostMessagesReq>();
            }
            auto messages = std::any_cast<std::shared_ptr<PostMessagesReq>>(routing.payload);
            for(auto & it : *payload.mutable_msg_list())
            {
                *messages->add_msg_list() = std::move(it);
            }
            routing.nums += payload.msg_list_size();
            return payload.msg_list_size();
        }
        size_t ParseDeleteMessages(const std::string& body)
        {
            messageSystem::DeleteMessagesReq payload;
            if(!payload.ParseFromString(body))
            {
                LOG_ERROR("解析DeleteMessagesReq失败!");
                return 0;
            }
            auto& routing = _routings[AMQP_MESSAGE_DELETE_ROUTING_KEY];
            if(!routing.payload.has_value())
            {
                routing.payload = std::make_shared<DeleteMessagesReq>();
            }
            auto messages = std::any_cast<std::shared_ptr<DeleteMessagesReq>>(routing.payload);
            for(auto& it : *payload.mutable_msg_list())
            {
                *messages->add_msg_list() = std::move(it);
            }
            routing.nums += payload.msg_list_size();
            return payload.msg_list_size();
        }
        void HandlerTransfer(RoutingInfo& routing_info)
        {
            routing_info.updateRetryTime();
            std::string routing = routing_info.routing;
            ServiceChannel::ChannelPtr channel;
            if(_services->chooseService(routing, &channel).status == false)
            {
                LOG_ERROR("服务异常:{}!",routing);
                return;
            }
            auto* cntl = new brpc::Controller();
            auto* rsp = new messageSystem::CommRsp();
            google::protobuf::Closure* done = brpc::NewCallback(this,&TaskTransferServer::asyncCallback,cntl,rsp,&routing_info);
            if(routing == AMQP_MESSAGE_POST_ROUTING_KEY)
            {
                auto req = std::any_cast<std::shared_ptr<PostMessagesReq>>(routing_info.payload);
                req->mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
                messageSystem::MsgStorageService_Stub stub(channel.get());
                stub.PostMessages(cntl, req.get(), rsp, done);
            }
            else if(routing == AMQP_MESSAGE_DELETE_ROUTING_KEY)
            {
                auto req = std::any_cast<std::shared_ptr<DeleteMessagesReq>>(routing_info.payload);
                req->mutable_request()->set_request_id(util::StringUtil::generateUniqueName());
                messageSystem::MsgStorageService_Stub stub(channel.get());
                stub.DeleteMessages(cntl, req.get(), rsp, done);
            }
        }
        void flushThreadWork()
        {
            LOG_INFO("任务刷新线程已经启动!");
            for(;;)
            {
                std::vector<TaskEntry> local_tasks;
                {
                    std::unique_lock<std::mutex> lock(*_mutex);
                    _cv->wait_for(lock, _config._flush_interval, [this]
                            {
                            return !_task_queue.empty();
                        });
                    while(!_task_queue.empty())
                    {
                        local_tasks.push_back(std::move(_task_queue.front()));
                        _task_queue.pop();
                    }
                }
                for(auto& task : local_tasks)
                {
                    TaskPayload payload;
                    if(!payload.ParseFromString(task.payload_bytes))
                    {
                        LOG_ERROR("解析TaskPayload失败!");
                        continue;
                    }
                    auto routing = payload.routing_key();
                    if(routing == AMQP_MESSAGE_POST_ROUTING_KEY)
                    {
                        if(_routings[routing].routing.empty())
                            _routings[routing].routing = routing;
                        size_t count = ParsePostMessages(payload.payload_bytes());
                        LOG_DEBUG("合并消息:routing={},count={}", routing, count);
                    }
                    else if(routing == AMQP_MESSAGE_DELETE_ROUTING_KEY)
                    {
                        if(_routings[routing].routing.empty())
                            _routings[routing].routing = routing;
                        size_t count = ParseDeleteMessages(payload.payload_bytes());
                        LOG_DEBUG("合并撤回任务:routing={},count={}", routing, count);
                    }
                    _routings[routing]._deliveryTags.push(task.deliveryTag);
                }
                std::vector<std::string> to_flush;
                {
                    std::lock_guard<std::mutex> lock(*_mutex);
                    for(auto& [key, info] : _routings)
                    {
                        if(info.nums >= _config.max_batch_size && !info.in_flush_queue)
                        {
                            info.in_flush_queue = true;
                            to_flush.push_back(key);
                        }
                    }
                }
                for(auto& routing : to_flush)
                {
                    HandlerTransfer(_routings[routing]);
                }
            }
        }
        void onTaskReceived(const std::string& payload, uint64_t deliveryTag)
        {
            std::lock_guard<std::mutex> lock(*_mutex);
            if(_task_queue.size() >= _config.internal_queue_capacity)
            {
                LOG_WARNING("任务队列迟滞(size):{}!",_task_queue.size());
                _mq->reject(deliveryTag, AMQP::requeue);
                return;
            }
            TaskEntry task;
            task.payload_bytes = payload;
            task.deliveryTag = deliveryTag;
            _task_queue.push(std::move(task));
            _cv->notify_one();
        }
    public:
        TaskTransferServer()
        :_retry_queue(this)
        {
            _mutex = std::make_shared<std::mutex>();
            _cv = std::make_shared<std::condition_variable>();
            _services = std::make_shared<ServiceManager>();
        }
        ~TaskTransferServer()
        {
            if(_flush_thread.joinable())
                _flush_thread.join();
        }
        void InitRabbitMQ(const std::string& user,const std::string& password,const std::string& host)
        {
            _mq = std::make_shared<RabbitMQ>(user,password,host,false);
        }
        void Register(const std::string& reg_host,const std::string& service_name,const std::string& access_host)
        {
            Registrant r(reg_host);
            r.Register(service_name, access_host);
        }
        void Discover(const std::string& reg_host,const std::string& access_host)
        {
            _discover = std::make_unique<Discovery>(reg_host,access_host
                ,std::bind(&TaskTransferServer::Put,this,std::placeholders::_1,std::placeholders::_2)
                ,std::bind(&TaskTransferServer::Del,this,std::placeholders::_1,std::placeholders::_2));
        }
        void AddRoutingService(const std::string& routing_key, const std::string& host)
        {
            std::lock_guard<std::mutex> lock(*_mutex);
            _services->addService(routing_key, host);
            _services->activeService(routing_key);
            _routings[routing_key].routing = routing_key;
            _routings[routing_key].host = host;
        }
        /// @brief 测试接口：直接注入任务到内部队列
        bool PushTaskForTest(const std::string& payload, uint64_t delivery_tag)
        {
            std::lock_guard<std::mutex> lock(*_mutex);
            if(_task_queue.size() >= _config.internal_queue_capacity)
            {
                return false;
            }
            _task_queue.push(TaskEntry{payload, delivery_tag});
            return true;
        }
        void FlushOnceForTest()
        {
            std::vector<TaskEntry> local_tasks;
            {
                std::lock_guard<std::mutex> lock(*_mutex);
                while(!_task_queue.empty())
                {
                    local_tasks.push_back(std::move(_task_queue.front()));
                    _task_queue.pop();
                }
            }
            for(auto& task : local_tasks)
            {
                TaskPayload payload;
                if(!payload.ParseFromString(task.payload_bytes))
                {
                    continue;
                }
                auto routing = payload.routing_key();
                if(routing == AMQP_MESSAGE_POST_ROUTING_KEY)
                {
                    if(_routings[routing].routing.empty())
                        _routings[routing].routing = routing;
                    ParsePostMessages(payload.payload_bytes());
                }
                else if(routing == AMQP_MESSAGE_DELETE_ROUTING_KEY)
                {
                    if(_routings[routing].routing.empty())
                        _routings[routing].routing = routing;
                    ParseDeleteMessages(payload.payload_bytes());
                }
                _routings[routing]._deliveryTags.push(task.deliveryTag);
            }
            std::vector<std::string> to_flush;
            {
                std::lock_guard<std::mutex> lock(*_mutex);
                for(auto& [key, info] : _routings)
                {
                    if(info.nums >= _config.max_batch_size && !info.in_flush_queue)
                    {
                        info.in_flush_queue = true;
                        to_flush.push_back(key);
                    }
                }
            }
            for(auto& routing : to_flush)
            {
                HandlerTransfer(_routings[routing]);
            }
        }
        void start()
        {
            _mq->addExchange(AMQP_MESSAGE_EXCHANGE, AMQP::direct, AMQP::durable);
            _mq->addQueue(AMQP_MESSAGE_POST_QUEUE, AMQP::durable);
            _mq->addQueue(AMQP_MESSAGE_DELETE_QUEUE, AMQP::durable);
            _mq->bind(AMQP_MESSAGE_EXCHANGE, AMQP_MESSAGE_POST_QUEUE, AMQP_MESSAGE_POST_ROUTING_KEY);
            _mq->bind(AMQP_MESSAGE_EXCHANGE, AMQP_MESSAGE_DELETE_QUEUE, AMQP_MESSAGE_DELETE_ROUTING_KEY);
            _mq->consume(AMQP_MESSAGE_POST_QUEUE,
                std::bind(&TaskTransferServer::onTaskReceived, this, std::placeholders::_1, std::placeholders::_2));
            _mq->consume(AMQP_MESSAGE_DELETE_QUEUE,
                std::bind(&TaskTransferServer::onTaskReceived, this, std::placeholders::_1, std::placeholders::_2));
            _flush_thread = std::thread(&TaskTransferServer::flushThreadWork, this);
            LOG_INFO("任务中转服务已经启动!");
            _mq->blockRun();
        }
        TaskRelayConfig& getConfig(){return _config;}
    private:
        void Put(const std::string& key,const std::string& value)
        {
            std::lock_guard<std::mutex> lock(*_mutex);
            _services->addService(key, value);
            _services->activeService(key);
            _routings[key].host = value;
        }
        void Del(const std::string& key,const std::string& value)
        {
            std::lock_guard<std::mutex> lock(*_mutex);
            _services->inactiveService(key);
        }
        class RetryQueue
        {
        private:
            void retryThreadWork()
            {
                LOG_INFO("任务重试服务已经启动!");
                for(;;)
                {
                    std::unique_lock<std::mutex> lock(*_server->_mutex);
                    _server->_cv->wait_for(lock, _server->_config._flush_interval, [this]
                    {
                        if(_active_retry_queue_ptr->empty())
                        {
                            swap(_active_retry_queue_ptr,_inactive_retry_queue_ptr);
                            return false;
                        }
                        return true;
                    });
                    while(_active_retry_queue_ptr->size())
                    {
                        auto routing = _active_retry_queue_ptr->front();
                        _active_retry_queue_ptr->pop();
                        if(routing.canRetry())
                            _server->HandlerTransfer(routing);
                        else
                            _inactive_retry_queue_ptr->push(routing);
                    }
                    swap(_active_retry_queue_ptr,_inactive_retry_queue_ptr);
                }
            }
        public:
            RetryQueue(TaskTransferServer* server)
            :_server(server),_retry_thread(std::bind(&RetryQueue::retryThreadWork,this))
            {
                _active_retry_queue_ptr = &_retry_queue1;
                _inactive_retry_queue_ptr = &_retry_queue2;
            }
            void pushRetryRouting(const RoutingInfo& routing)
            {
                _inactive_retry_queue_ptr->push(routing);
            }
            thread::id getThreadId()const{return _retry_thread.get_id();}
            ~RetryQueue()
            {
                if(_retry_thread.joinable())
                    _retry_thread.join();
            }
        private:
            TaskTransferServer* _server;
            std::thread _retry_thread;
            std::queue<RoutingInfo> _retry_queue1;
            std::queue<RoutingInfo> _retry_queue2;
            std::queue<RoutingInfo>* _active_retry_queue_ptr;
            std::queue<RoutingInfo>* _inactive_retry_queue_ptr;
        };
    private:
        TaskRelayConfig _config;
        std::shared_ptr<RabbitMQ> _mq;
        std::thread _flush_thread;
        std::queue<TaskEntry> _task_queue;
        std::queue<std::string> _to_flush_queue;
        std::unordered_map<std::string, RoutingInfo> _routings;
        std::shared_ptr<ServiceManager> _services;
        RetryQueue _retry_queue;
        std::unique_ptr<Discovery> _discover;
        std::shared_ptr<std::mutex> _mutex;
        std::shared_ptr<std::condition_variable> _cv;
    };
}
