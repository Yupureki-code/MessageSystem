#include "conversation_server.hpp"
#include <thread>
#include <chrono>

DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

DEFINE_string(instance_name, "/conversation_service/instance", "当前实例名称");
DEFINE_string(access_host, "127.0.0.1:8003", "当前实例的外部访问地址");

DEFINE_int32(listen_port, 8003, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");

using namespace messageSystem;

int main()
{
    messageSystem::initLogger("conversation_logger", FLAGS_log_file, spdlog::level::debug);
    messageSystem::ConversationServerWrapper server(REGISTRY_HOST,FLAGS_access_host);
    server.InitDB(MYSQL_USER, MYSQL_PASSWD, MYSQL_DB, MYSQL_HOST, MYSQL_SERVER_PORT);
    server.Start(FLAGS_listen_port, FLAGS_rpc_timeout, FLAGS_rpc_threads);
    while(true) std::this_thread::sleep_for(std::chrono::seconds(60));
    return 0;
}