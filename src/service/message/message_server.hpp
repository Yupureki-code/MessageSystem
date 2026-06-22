#pragma once
#include "../../comm_include/proto_include/message_store.pb.h"
#include "../../comm_include/proto_include/conversation.pb.h"
#include "../../comm_include/proto_include/message.pb.h"
#include "../../comm_include/proto_include/task_transfer.pb.h"
#include <brpc/controller.h>
#include <brpc/server.h>
#include <functional>
#include <memory>
#include <sw/redis++/utils.h>
#include "../../comm_include/redis.hpp"
#include "../../comm_include/channel.hpp"
#include "../../comm_include/etcd.hpp"
#include "../../comm_include/rabbitmq.hpp"

namespace messageSystem
{
    class MessageServerImpl : public MessageServer
    {
    private:
        Response RedisHGet(const std::string& hash, const std::string& key, sw::redis::OptionalString* value)
        {
            if(_redis_hget_fn)
            {
                return _redis_hget_fn(hash, key, &value->value());
            }
            return _redis->hget(hash, key, value);
        }
        Response RedisHSet(const std::string& hash, const std::string& key, const std::string& value)
        {
            if(_redis_hset_fn)
            {
                return _redis_hset_fn(hash, key, value);
            }
            return _redis->hset(hash, key, value);
        }
        Response RedisHIncrBy(const std::string& hash, const std::string& key, int delta)
        {
            if(_redis_hincrby_fn)
            {
                return _redis_hincrby_fn(hash, key, delta);
            }
            return _redis->hincrby(hash, key, delta);
        }
        bool PublishTask(const std::string& exchange, const std::string& body, const std::string& routing_key)
        {
            if(_publish_task_fn)
            {
                return _publish_task_fn(exchange, body, routing_key);
            }
            return _mq->publish(exchange, body, routing_key);
        }
        void HandlerError(CommRsp* rep,const std::string & rid,bool status,const std::string& msg)
        {
            rep->set_request_id(rid);
            rep->set_status(status);
            rep->set_errmsg(msg);
        }
        bool FetchConversationMembers(const std::string& rid,
            const std::string& conversation_id,
            std::vector<UserInfo>* user_infos)
        {
            if(_fetch_members_fn)
            {
                return _fetch_members_fn(rid, conversation_id, user_infos);
            }
            ServiceChannel::ChannelPtr channel;
            Response return_rep = _services->chooseService(CONVERSATION_SERVICE, &channel);
            if(!return_rep.status)
            {
                LOG_ERROR("{} - {}", rid, return_rep.errmsg);
                return false;
            }

            messageSystem::ConversationServer_Stub stub(channel.get());
            messageSystem::GetConversationMemberListReq req;
            messageSystem::GetConversationMemberListRsp rsp;
            req.mutable_request()->set_request_id(rid);
            req.set_conversaion_id(conversation_id);

            brpc::Controller cntl;
            stub.GetConversationMemberList(&cntl, &req, &rsp, nullptr);
            if(cntl.Failed() || !rsp.response().status())
            {
                LOG_ERROR("{} - 获取会话成员失败:{}", rid, cntl.Failed() ? cntl.ErrorText() : rsp.response().errmsg());
                return false;
            }

            user_infos->clear();
            for(const auto& user_info : rsp.user_infos())
            {
                user_infos->push_back(user_info);
            }
            return true;
        }
        bool PublishDeleteTask(const std::string& rid, const MessageInfo& message)
        {
            messageSystem::DeleteMessagesReq delete_req;
            delete_req.mutable_request()->set_request_id(rid);
            *delete_req.add_msg_list() = message;

            messageSystem::TaskPayload task;
            task.set_routing_key(AMQP_MESSAGE_DELETE_ROUTING_KEY);
            task.set_payload_bytes(delete_req.SerializeAsString());
            task.set_task_id(util::StringUtil::generateUniqueName());
            task.set_trace_id(rid);
            task.set_created_ts(util::TimeUtil::getCurrentTimestampSeconds());
            task.set_retry_count(0);

            return PublishTask(AMQP_MESSAGE_EXCHANGE, task.SerializeAsString(), AMQP_MESSAGE_DELETE_ROUTING_KEY);
        }
    public:
        void InitRedis(std::shared_ptr<redis::RedisClient> redis)
        {
            _redis = redis;
        }
        void InitService(std::shared_ptr<ServiceManager> services)
        {
            _services = services;
        }
        void InitRabbitMQ(std::shared_ptr<RabbitMQ> mq)
        {
            _mq = mq;
        }
        void SetRedisHGetForTest(std::function<Response(const std::string&, const std::string&, std::string*)> fn)
        {
            _redis_hget_fn = std::move(fn);
        }
        void SetRedisHSetForTest(std::function<Response(const std::string&, const std::string&, const std::string&)> fn)
        {
            _redis_hset_fn = std::move(fn);
        }
        void SetRedisHIncrByForTest(std::function<Response(const std::string&, const std::string&, int)> fn)
        {
            _redis_hincrby_fn = std::move(fn);
        }
        void SetPublishForTest(std::function<bool(const std::string&, const std::string&, const std::string&)> fn)
        {
            _publish_task_fn = std::move(fn);
        }
        void SetFetchConversationMembersForTest(
            std::function<bool(const std::string&, const std::string&, std::vector<UserInfo>*)> fn)
        {
            _fetch_members_fn = std::move(fn);
        }
        virtual void SendMessage(::google::protobuf::RpcController* controller,
            const ::messageSystem::SendMessageReq* request,
            ::messageSystem::SendMessageRsq* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到消息发送请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            MessageInfo message = request->message();
            CommRsp* rep = response->mutable_response();
            std::vector<UserInfo> user_infos;
            if(!FetchConversationMembers(rid, message.conversation_id(), &user_infos))
            {
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            for(auto & it : user_infos)
            {
                std::string str = "user:" + it.user_id();
                sw::redis::OptionalString value;
                Response return_rep = RedisHGet(str, "online", &value);
                if(!return_rep.status)
                {
                    LOG_ERROR("{} - {}",rid,return_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
                if(value == "true")
                {
                    response->add_uids(it.user_id());
                }
                else 
                {
                    // 离线用户该会话未读消息加1
                    std::string str = "user:" + it.user_id();
                    RedisHIncrBy(str, "unread:" + message.conversation_id(), 1);
                }
            }
            messageSystem::PostMessagesReq message_store_req;
            message_store_req.mutable_request()->set_request_id(rid);
            *message_store_req.add_msg_list() = message;

            messageSystem::TaskPayload task;
            task.set_routing_key(AMQP_MESSAGE_POST_ROUTING_KEY);
            task.set_payload_bytes(message_store_req.SerializeAsString());
            task.set_task_id(util::StringUtil::generateUniqueName());
            task.set_trace_id(rid);
            task.set_created_ts(util::TimeUtil::getCurrentTimestampSeconds());
            task.set_retry_count(0);

            if(!PublishTask(AMQP_MESSAGE_EXCHANGE, task.SerializeAsString(), AMQP_MESSAGE_POST_ROUTING_KEY))
            {
                LOG_ERROR("{} - 发布发送消息任务失败!", rid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }
            rep->set_request_id(rid);
            rep->set_status(true);
        }
        virtual void RecallMessage(::google::protobuf::RpcController* controller,
            const ::messageSystem::RecallMessageReq* request,
            ::messageSystem::RecallMessageRsp* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到消息撤回请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            MessageInfo message = request->message();
            CommRsp* rep = response->mutable_response();

            std::vector<UserInfo> user_infos;
            if(!FetchConversationMembers(rid, message.conversation_id(), &user_infos))
            {
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }

            for(const auto& user_info : user_infos)
            {
                std::string redis_key = "user:" + user_info.user_id();
                sw::redis::OptionalString online_value;
                Response redis_rep = RedisHGet(redis_key, "online", &online_value);
                if(!redis_rep.status)
                {
                    LOG_ERROR("{} - {}", rid, redis_rep.errmsg);
                    HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                    return;
                }
                if(online_value == "true")
                {
                    response->add_uids(user_info.user_id());
                }
            }

            if(!PublishDeleteTask(rid, message))
            {
                LOG_ERROR("{} - 发布撤回任务失败!", rid);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }

            Response seq_rep = RedisHIncrBy("conversation:" + message.conversation_id(), "seq", 1);
            if(!seq_rep.status)
            {
                LOG_ERROR("{} - 更新会话seq失败:{}", rid, seq_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }

            rep->set_request_id(rid);
            rep->set_status(true);
        }
        virtual void MarkAsRead(::google::protobuf::RpcController* controller,
            const ::messageSystem::MarkAsReadReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到消息已读请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request().request_id();
            std::string conversation_id = request->coversation_id();
            std::string user_id = request->request().uid();
            CommRsp* rep = response;

            Response redis_rep = RedisHSet("user:" + user_id, "unread:" + conversation_id, "0");
            if(!redis_rep.status)
            {
                LOG_ERROR("{} - 清零未读数失败:{}", rid, redis_rep.errmsg);
                HandlerError(rep, rid, false, "服务器繁忙，请稍后重试!");
                return;
            }

            rep->set_request_id(rid);
            rep->set_status(true);
        }
    private:
        std::shared_ptr<redis::RedisClient> _redis;
        std::shared_ptr<ServiceManager> _services;
        std::shared_ptr<RabbitMQ> _mq;
        std::function<Response(const std::string&, const std::string&, std::string*)> _redis_hget_fn;
        std::function<Response(const std::string&, const std::string&, const std::string&)> _redis_hset_fn;
        std::function<Response(const std::string&, const std::string&, int)> _redis_hincrby_fn;
        std::function<bool(const std::string&, const std::string&, const std::string&)> _publish_task_fn;
        std::function<bool(const std::string&, const std::string&, std::vector<UserInfo>*)> _fetch_members_fn;
    };
    class MessageServerWrapper
    {
    private:
        void Put(const std::string& key, const std::string& value)
        {
            if(key == _conversation_service_base_url)
            {
                _services->addService(CONVERSATION_SERVICE, value);
                _services->activeService(key);
            }
        }
        void Del(const std::string& key, const std::string& value)
        {
            if(key == _conversation_service_base_url)
            {
                _services->inactiveService(key);
            }
        }
    public:
        MessageServerWrapper(const std::string& registry_host, const std::string& access_dir,
            const std::string& conversation_service_base_url = SERVICE_BASE_URL + CONVERSATION_SERVICE)
        :_conversation_service_base_url(conversation_service_base_url)
        ,_discover(registry_host, access_dir,
            std::bind(&MessageServerWrapper::Put, this, std::placeholders::_1, std::placeholders::_2),
            std::bind(&MessageServerWrapper::Del, this, std::placeholders::_1, std::placeholders::_2))
        {
            _services = std::make_shared<ServiceManager>();
            _server.InitService(_services);
        }
        void InitRedis(const std::string& ip, int port, int thread_size, int late_time)
        {
            std::shared_ptr<redis::RedisClient> redis = std::make_shared<redis::RedisClient>(ip, port, thread_size, late_time);
            _server.InitRedis(redis);
        }
        void InitRabbitMQ(const std::string& user, const std::string& password, const std::string& host)
        {
            std::shared_ptr<RabbitMQ> mq = std::make_shared<RabbitMQ>(user, password, host, false);
            _server.InitRabbitMQ(mq);
            mq->addExchange(AMQP_MESSAGE_EXCHANGE, AMQP::direct, AMQP::durable);
            mq->addQueue(AMQP_MESSAGE_POST_QUEUE, AMQP::durable);
            mq->addQueue(AMQP_MESSAGE_DELETE_QUEUE, AMQP::durable);
            mq->bind(AMQP_MESSAGE_EXCHANGE, AMQP_MESSAGE_POST_QUEUE, AMQP_MESSAGE_POST_ROUTING_KEY);
            mq->bind(AMQP_MESSAGE_EXCHANGE, AMQP_MESSAGE_DELETE_QUEUE, AMQP_MESSAGE_DELETE_ROUTING_KEY);
        }
        void Start(uint16_t port, int32_t timeout, uint8_t num_threads)
        {
            _rpc_server = std::make_unique<brpc::Server>();
            int ret = _rpc_server->AddService(&_server, brpc::ServiceOwnership::SERVER_OWNS_SERVICE);
            if(ret == -1)
            {
                LOG_ERROR("添加RPC服务失败!");
                return;
            }
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout;
            options.num_threads = num_threads;
            ret = _rpc_server->Start(port, &options);
            if(ret == -1)
            {
                LOG_ERROR("服务启动失败!");
                return;
            }
        }
    private:
        std::unique_ptr<brpc::Server> _rpc_server;
        std::string _conversation_service_base_url;
        Discovery _discover;
        MessageServerImpl _server;
        std::shared_ptr<ServiceManager> _services;
    };
}