#include "../../comm_include/proto_include/message_store.pb.h"
#include "../../comm_include/proto_include/message.pb.h"
#include <brpc/server.h>
#include "../../comm_include/es.hpp"
#include "../../comm_include/redis.hpp"
#include "../../comm_include/mail.hpp"
#include "../../comm_include/channel.hpp"
#include "../../comm_include/proto_include/file.pb.h"
#include "../../comm_include/etcd.hpp"

namespace messageSystem
{
    class MessageServerImpl : MessageServer
    {
    public:
        virtual void SendMessage(::google::protobuf::RpcController* controller,
            const ::messageSystem::SendMessageReq* request,
            ::messageSystem::CommRsp* response,
            ::google::protobuf::Closure* done)
        {
            LOG_DEBUG("收到消息发送请求！");
            brpc::ClosureGuard rpc_guard(done);
            std::string rid = request->request_id();
        }
    private:
    };
}