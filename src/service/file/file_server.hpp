#pragma once
#include "../../comm_include/proto_include/file.pb.h"
#include "../../comm_include/filesystem.hpp"
#include "../../comm_include/comm.hpp"
#include "../../comm_include/etcd.hpp"
#include "../../comm_include/channel.hpp"
#include <brpc/server.h>
#include <cerrno>

namespace messageSystem
{
    class FileServiceImpl : public messageSystem::FileService 
    {
    private:
        void HandlerError(const ::messageSystem::GetFileReq* request,::messageSystem::GetFileRsp* response,const fileUtil::Response& rep)
        {
            response->set_success(false);
            response->set_errmsg(rep.errmsg);
            LOG_ERROR("{} {}!", request->request().request_id(),rep.errmsg);
        }
        void HandlerError(CommRsp* response, const std::string& request_id, const std::string& errmsg)
        {
            response->set_status(false);
            response->set_errmsg(errmsg);
            response->set_request_id(request_id);
            LOG_ERROR("{} {}!", request_id, errmsg);
        }
    public:
        FileServiceImpl(const std::string &storage_path):
            _storage_path(storage_path)
        {
            umask(0);
            mkdir(storage_path.c_str(), 0775);
            if (_storage_path.back() != '/') _storage_path.push_back('/');
        }
        ~FileServiceImpl(){}
        void GetSingleFile(google::protobuf::RpcController* controller,
                    const ::messageSystem::GetFileReq* request,
                    ::messageSystem::GetFileRsp* response,
                    ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            response->set_request_id(request->request().request_id());
            //1. 取出请求中的文件ID（起始就是文件名）
            std::string fid = request->file_id_list(0);
            std::string filename = _storage_path + fid;
            //2. 将文件ID作为文件名，读取文件数据
            std::string body;
            FileDownloadData data;
            data.set_file_id(fid);
            auto ret = _file.read(_storage_path, fid, *data.mutable_file_content());
            if (ret.status == false) 
            {
                HandlerError(request, response, ret);
                return;
            }
            //3. 组织响应
            response->set_success(true);
            response->mutable_file_data()->insert({fid,data});
        }
        void GetMultiFile(google::protobuf::RpcController* controller,
                    const ::messageSystem::GetFileReq* request,
                    ::messageSystem::GetFileRsp* response,
                    ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            response->set_request_id(request->request().request_id());
            // 循环取出请求中的文件ID，读取文件数据进行填充
            for (int i = 0; i < request->file_id_list_size(); i++) 
            {
                std::string fid = request->file_id_list(i);
                std::string filename = _storage_path + fid;
                FileDownloadData data;
                data.set_file_id(fid);
                auto ret = _file.read(_storage_path, fid, *data.mutable_file_content());
                if (ret.status == false) 
                {
                    HandlerError(request, response, ret);
                    return;
                }
                response->mutable_file_data()->insert({fid, data});
            }
            response->set_success(true);
        }
        void PutSingleFile(google::protobuf::RpcController* controller,
                    const ::messageSystem::PutFileReq* request,
                    ::messageSystem::CommRsp* response,
                    ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            response->set_request_id(request->request().request_id());
            //1. 为文件生成一个唯一uudi作为文件名 以及 文件ID
            std::string fid = fileUtil::FileSystem::generateUniqueFileName();
            std::string filename = _storage_path + fid;
            //2. 取出请求中的文件数据，进行文件数据写入
            auto ret = _file.write(_storage_path, fid,request->file_data(0).file_content());
            if (ret.status == false) 
            {
                HandlerError(response, request->request().request_id(), ret.errmsg);
                return;
            }
            //3. 组织响应
            response->set_status(true);
        }
        void PutMultiFile(google::protobuf::RpcController* controller,
                    const ::messageSystem::PutFileReq* request,
                    ::messageSystem::CommRsp* response,
                    ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            response->set_request_id(request->request().request_id());
            for (int i = 0; i < request->file_data_size(); i++) 
            {
                std::string fid = fileUtil::FileSystem::generateUniqueFileName();
                std::string filename = _storage_path + fid;
                auto ret = _file.write(_storage_path, fid, request->file_data(i).file_content());
                if (ret.status == false) 
                {
                    HandlerError(response, request->request().request_id(), ret.errmsg);
                    return;
                }
            }
            response->set_status(true);
        }
        void DeleteMultiFile(google::protobuf::RpcController* controller,
                    const ::messageSystem::DeleteFileReq* request,
                    ::messageSystem::CommRsp* response,
                    ::google::protobuf::Closure* done) 
        {
            brpc::ClosureGuard rpc_guard(done);
            response->set_request_id(request->request().request_id());
            //延迟删除：将文件移动到延迟删除目录，24小时后由定时任务清理
            std::string delay_path = _storage_path + "delay_delete/";
            umask(0);
            mkdir(delay_path.c_str(), 0775);
            
            for (int i = 0; i < request->file_id_list_size(); i++) 
            {
                std::string fid = request->file_id_list(i);
                std::string src_path = _storage_path + fid;
                std::string dst_path = delay_path + fid;
                //使用rename进行原子操作，如果文件不存在则跳过
                if (rename(src_path.c_str(), dst_path.c_str()) != 0) 
                {
                    if (errno != ENOENT) 
                    {
                        LOG_ERROR("移动文件到延迟删除目录失败:{}!", fid);
                        HandlerError(response, request->request().request_id(), "删除文件失败");
                        return;
                    }
                }
            }
            response->set_status(true);
        }
    private:
        fileUtil::FileSystem _file;
        std::string _storage_path;
    };
    class FileServer
    {
    public:
        FileServer(const std::string& reg_host,const std::string& path)
        :_reg(reg_host),_server(path)
        {    
        }
        void Register(const std::string service_name,const std::string&host)
        {
            _reg.Register(service_name, host);
        }
        void start(uint16_t port, int32_t timeout, uint8_t num_threads)
        {
            _rpc_server = std::make_unique<brpc::Server>();
            int ret = _rpc_server->AddService(&_server,brpc::ServiceOwnership::SERVER_OWNS_SERVICE);
            if(ret == -1)
            {
                LOG_ERROR("添加RPC服务失败!");
                return;
            }
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout;
            options.num_threads = num_threads;
            ret = _rpc_server->Start(port,&options);
            if(ret == -1)
            {
                LOG_ERROR("服务启动失败!");
                return;
            }
        }
    private:
        std::unique_ptr<brpc::Server> _rpc_server;
        Registrant _reg;
        FileServiceImpl _server;
    };
}