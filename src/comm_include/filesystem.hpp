#pragma once
#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <atomic>

namespace fileUtil
{
    // 响应结构体
struct Response {
    bool status;         // 0 表示成功，负值表示错误
    std::string errmsg; // 错误信息
    
    Response() : status(true) {}
    Response(int s, const std::string& msg = "") : status(s), errmsg(msg) {}
};

// 高性能文件 I/O 类
class FileSystem {
public:
// 生成唯一文件名：年月日时分秒-毫秒级时间戳 + 原子计数器
    static std::string generateUniqueFileName() 
    {
        static std::atomic<uint64_t> counter{0};
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()) % 1000000;
        
        std::tm tm = *std::localtime(&time_t);
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y%m%d%H%M%S") 
            << "-" << std::setfill('0') << std::setw(6) << us.count()
            << "-" << counter.fetch_add(1);
        return oss.str();
    }
    // 读取文件内容
    Response read(const std::string& filePath, 
                  const std::string& fileName, 
                  std::string& content) 
    {
        try {
            std::string fullPath = filePath + fileName;
            
            // 检查文件是否存在
            if (!std::filesystem::exists(fullPath)) {
                return Response(false, "文件不存在: " + fullPath);
            }
            
            // 获取文件大小
            auto fileSize = std::filesystem::file_size(fullPath);
            
            // 根据文件大小选择读取策略
            if (fileSize > 1024 * 1024) {  // 大于 1MB 使用 mmap
                return readWithMmap(fullPath, content);
            } else {  // 小文件使用传统读取
                return readWithTraditional(fullPath, content);
            }
        } catch (const std::exception& e) {
            return Response(false, std::string("读取文件异常: ") + e.what());
        }
    }
    
    // 写入文件内容
    Response write(const std::string& filePath, 
                    const std::string& fileName,
                   const std::string& content) 
    {
        try {
            // 生成唯一文件名
            std::string fullPath = filePath + fileName;
            
            // 确保目录存在
            if (!std::filesystem::exists(filePath)) {
                std::filesystem::create_directories(filePath);
            }
            
            // 根据内容大小选择写入策略
            if (content.size() > 1024 * 1024) {  // 大于 1MB 使用 mmap
                return writeWithMmap(fullPath, content);
            } else {  // 小文件使用传统写入
                return writeWithTraditional(fullPath, content);
            }
        } catch (const std::exception& e) {
            return Response(false, std::string("写入文件异常: ") + e.what());
        }
    }

private:
    
    // 使用 mmap 读取文件
    Response readWithMmap(const std::filesystem::path& path, std::string& content) {
        // 打开文件
        int fd = open(path.c_str(), O_RDONLY);
        if (fd == -1) {
            return Response(false, "打开文件失败: " + std::string(strerror(errno)));
        }
        
        // 获取文件大小
        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            close(fd);
            return Response(false, "获取文件信息失败: " + std::string(strerror(errno)));
        }
        
        // 内存映射
        void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (addr == MAP_FAILED) {
            close(fd);
            return Response(-1, "内存映射失败: " + std::string(strerror(errno)));
        }
        
        // 预读优化：提前加载数据到内存
        madvise(addr, sb.st_size, MADV_SEQUENTIAL);
        
        // 复制数据到 string
        content.assign(static_cast<char*>(addr), sb.st_size);
        
        // 清理资源
        munmap(addr, sb.st_size);
        close(fd);
        
        return Response(true);
    }
    
    // 使用 mmap 写入文件
    Response writeWithMmap(const std::filesystem::path& path, const std::string& content) {
        // 创建文件
        int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) {
            return Response(false, "创建文件失败: " + std::string(strerror(errno)));
        }
        
        // 设置文件大小
        if (ftruncate(fd, content.size()) == -1) {
            close(fd);
            return Response(false, "设置文件大小失败: " + std::string(strerror(errno)));
        }
        
        // 内存映射
        void* addr = mmap(nullptr, content.size(), PROT_WRITE, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            close(fd);
            return Response(false, "内存映射失败: " + std::string(strerror(errno)));
        }
        
        // 复制数据到映射区域
        std::memcpy(addr, content.data(), content.size());
        
        // 同步到磁盘
        msync(addr, content.size(), MS_SYNC);
        
        // 清理资源
        munmap(addr, content.size());
        close(fd);
        
        return Response(true);
    }
    
    // 传统方式读取文件
    Response readWithTraditional(const std::filesystem::path& path, std::string& content) {
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            return Response(false, "打开文件失败: " + path.string());
        }
        
        // 获取文件大小
        auto fileSize = file.tellg();
        file.seekg(0, std::ios::beg);
        
        // 预分配内存
        content.resize(fileSize);
        
        // 读取文件内容
        if (!file.read(content.data(), fileSize)) {
            return Response(false, "读取文件失败: " + path.string());
        }
        
        return Response(true);
    }
    
    // 传统方式写入文件
    Response writeWithTraditional(const std::filesystem::path& path, const std::string& content) {
        std::ofstream file(path, std::ios::binary);
        if (!file) {
            return Response(false, "创建文件失败: " + path.string());
        }
        
        // 写入文件内容
        if (!file.write(content.data(), content.size())) {
            return Response(false, "写入文件失败: " + path.string());
        }
        
        return Response(true);
    }
    };
}
