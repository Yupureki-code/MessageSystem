#include "task_transfer_server.hpp"
#include <thread>
#include <chrono>

DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

DEFINE_string(registry_host, "http://127.0.0.1:2379", "服务注册中心地址");
DEFINE_string(base_service, "/service", "服务监控根目录");
DEFINE_string(instance_name, "/task_transfer_service/instance", "当前实例名称");
DEFINE_string(access_host, "127.0.0.1:8007", "当前实例的外部访问地址");

DEFINE_int32(listen_port, 8007, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 3, "Rpc的IO线程数量");

DEFINE_string(mq_user, "guest", "消息队列用户名");
DEFINE_string(mq_password, "guest", "消息队列密码");
DEFINE_string(mq_host, "127.0.0.1:5671", "消息队列地址");


DEFINE_string(es_host, "http://127.0.0.1:9200/", "ES搜索引擎服务器URL");

int main(int argc,char* argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    messageSystem::initLogger("task_transfer_logger", FLAGS_log_file, spdlog::level::debug);
    messageSystem::TaskTransferServer server;
    server.InitRabbitMQ(FLAGS_mq_user,FLAGS_mq_password,FLAGS_mq_host);
    server.Register(FLAGS_registry_host,FLAGS_base_service + FLAGS_instance_name , FLAGS_access_host);
    server.Discover(FLAGS_registry_host, FLAGS_access_host);
    server.start();
    while(true) std::this_thread::sleep_for(std::chrono::seconds(60));
    return 0;
}