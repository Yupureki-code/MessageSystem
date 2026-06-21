#pragma once
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <brpc/channel.h>
#include <functional>
#include <google/protobuf/stubs/callback.h>

namespace messageSystem 
{
    using CallBack = std::function<void(::google::protobuf::Closure *)>;

    template<typename Handler>
    struct AsioClosure : ::google::protobuf::Closure 
    {
        boost::asio::io_context& ioc;
        Handler handler;
        bool done = false;

        AsioClosure(boost::asio::io_context& io, Handler h) 
            : ioc(io), handler(std::move(h)) {}

        void Run() override 
        {
            if (!done) {
                done = true;
                boost::asio::post(ioc, [h = std::move(handler)]() mutable {
                    h();
                });
                delete this;
            }
        }

        ~AsioClosure() override = default;
    };

    inline boost::asio::awaitable<void> runInLoop(
        brpc::Controller* cntl, 
        boost::asio::io_context& ioc, 
        CallBack cb)
    {
        co_await boost::asio::async_initiate<void()>(
            [&ioc, &cb](auto handler) {
                auto* closure = new AsioClosure<decltype(handler)>(
                    ioc, std::move(handler));
                cb(closure);
            }, 
            boost::asio::use_awaitable);
    }
}
