#include "gateway.hpp"
#include <brpc/callback.h>
#include <brpc/controller.h>
#include <httplib.h>
#include <thread>
#include <chrono>

DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

DEFINE_string(instance_name, "/gateway/instance", "当前实例名称");
DEFINE_string(access_host, "127.0.0.1:8000", "当前实例的外部访问地址");

DEFINE_int32(rpc_port, 8000, "Rpc服务器监听端口");
DEFINE_int32(ws_port, 9000, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 4, "Rpc的IO线程数量");

using namespace messageSystem;

int main()
{
    messageSystem::initLogger("gateway_logger", FLAGS_log_file, spdlog::level::debug);
    messageSystem::GatwayServer server(REGISTRY_HOST,FLAGS_access_host);
    server.InitRedis(REDIS_HOST, REDIS_PORT, 10, 5);
    server.Start(FLAGS_rpc_port,FLAGS_ws_port, FLAGS_rpc_timeout, FLAGS_rpc_threads);
    while(true) std::this_thread::sleep_for(std::chrono::seconds(60));
    return 0;
}