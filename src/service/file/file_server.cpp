#include "file_server.hpp"
#include <spdlog/common.h>

DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

DEFINE_string(registry_host, "http://127.0.0.1:2379", "服务注册中心地址");
DEFINE_string(base_service, "/service", "服务监控根目录");
DEFINE_string(instance_name, "/file_service/instance", "当前实例名称");
DEFINE_string(access_host, "127.0.0.1:8001", "当前实例的外部访问地址");

DEFINE_string(storage_path, GetEnvOrDefault("DATA_PATH",DATA_PATH), "当前实例的外部访问地址");

DEFINE_int32(listen_port, 8001, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");

int main(int argc,char* argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    messageSystem::initLogger("file_server_logger", FLAGS_log_file, spdlog::level::debug);
    messageSystem::FileServer server(FLAGS_registry_host,FLAGS_storage_path);
    server.Register(FLAGS_base_service + FLAGS_instance_name, FLAGS_access_host);
    server.start(FLAGS_listen_port, FLAGS_rpc_timeout, FLAGS_rpc_threads);
}