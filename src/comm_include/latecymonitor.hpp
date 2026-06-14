#pragma once

#include <sstream>
#include <string>
#include <atomic>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include "filesystem.hpp"
#include "logger.hpp"

namespace latecyMonitor
{
    using namespace fileUtil;
    using namespace messageSystem;
    
    class LatencyMonitor
    {
    private:
        // 延迟记录结构
        struct LatencyRecord
        {
            std::string operation;  // 操作类型
            int64_t latency_ms;     // 延迟（毫秒）
            int64_t timestamp;      // 时间戳（毫秒）

            LatencyRecord(const std::string& op, int64_t lat, int64_t ts)
                : operation(op), latency_ms(lat), timestamp(ts) {}
        };
        // 内部方法
        void worker_thread_func()
        {
            while (running_)
            {
                std::vector<LatencyRecord> batch;

                {
                    std::unique_lock<std::mutex> lock(queue_mutex_);

                    // 等待：队列有数据或定时刷新或停止
                    cv_.wait_for(lock, flush_interval_, [this]
                        {
                        return !queue_.empty() || !running_;
                    });

                    // 收集批量数据
                    while (!queue_.empty() && batch.size() < batch_size_)
                    {
                        batch.push_back(std::move(queue_.front()));
                        queue_.pop();
                    }
                }

                // 写入文件
                if (!batch.empty())
                {
                    write_batch(batch);
                }
            }

            // 退出前处理剩余数据
            std::vector<LatencyRecord> remaining;
            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                while (!queue_.empty()) {
                    remaining.push_back(std::move(queue_.front()));
                    queue_.pop();
                }
            }

            if (!remaining.empty())
            {
                if(!write_batch(remaining))
                    return;
            }
        }

        bool write_batch(const std::vector<LatencyRecord>& batch)
        {
            std::lock_guard<std::mutex> lock(file_mutex_);
            std::stringstream ss;
            for (const auto& record : batch) {
                // 格式：时间戳,操作,延迟ms
                ss << record.timestamp << ","
                            << record.operation << ","
                            << record.latency_ms << "\n";
            }

            auto rep = _file.append(_file_path, _file_name, ss.str());
            if(!rep.status)
            {
                LOG_ERROR("{}",rep.errmsg);
                return false;
            }
            return true;
        }

        int64_t current_timestamp_ms() const {
            auto now = std::chrono::system_clock::now();
            auto duration = now.time_since_epoch();
            return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        }

    public:
        LatencyMonitor() = default;

        ~LatencyMonitor() {
            stop();
        }

        // 禁用拷贝和移动
        LatencyMonitor(const LatencyMonitor&) = delete;
        LatencyMonitor& operator=(const LatencyMonitor&) = delete;

        // 启动监控
        bool start()
        {
            if (running_)
            {
                return false;
            }

            running_ = true;

            // 启动后台线程
            worker_thread_ = std::thread(&LatencyMonitor::worker_thread_func, this);

            return true;
        }

        // 停止监控
        void stop()
        {
            if (!running_) {
                return;
            }

            running_ = false;
            enabled_ = false;

            cv_.notify_all();

            if (worker_thread_.joinable()) {
                worker_thread_.join();
            }
        }

        // 开关监控
        void enable(bool on)
        {
            enabled_ = on;
        }

        // 设置输出文件
        bool setOutputFile(const std::string& file_path, const std::string& file_name)
        {
            _file_path = file_path;
            _file_name = file_name;
            if (running_)
            {
                // 如果正在运行，先停止再重启
                stop();
                return start();
            }
            return true;
        }

        // 记录延迟
        void record(const std::string& operation, int64_t latency_ms)
        {
            if (!enabled_ || !running_)
            {
                return;
            }

            LatencyRecord record(operation, latency_ms, current_timestamp_ms());

            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                queue_.push(std::move(record));
            }

            cv_.notify_one();
        }

        // 获取状态
        bool is_enabled() const { return enabled_; }
        bool is_running() const { return running_; }
        size_t queue_size() const {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            return queue_.size();
        }

        // 配置批量写入
        void set_batch_size(size_t size) { batch_size_ = size; }
        void set_flush_interval(std::chrono::milliseconds interval)
        {
            flush_interval_ = interval;
        }
    private:
        // 控制开关
        std::atomic<bool> enabled_{false};
        std::atomic<bool> running_{false};

        // 队列和同步
        std::queue<LatencyRecord> queue_;
        mutable std::mutex queue_mutex_;
        std::condition_variable cv_;

        // 后台线程
        std::thread worker_thread_;

        // 文件输出
        std::string _file_path;
        std::string _file_name;
        FileSystem _file;
        mutable std::mutex file_mutex_;

        // 批量写入配置
        size_t batch_size_ = 100;      // 批量写入大小
        std::chrono::milliseconds flush_interval_{1000};  // 定时刷新间隔
    };
    class Timer 
    {  
    public:
        Timer(LatencyMonitor& monitor, const std::string& operation)
            : monitor_(monitor), operation_(operation), enabled_(monitor.is_enabled()) 
        {
            if (enabled_) 
            {
                start_ = std::chrono::high_resolution_clock::now();
            }
        }
        
        ~Timer() 
        {
            if (enabled_) {
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                    end - start_).count();
                monitor_.record(operation_, duration);
            }
        }
    private:
        LatencyMonitor& monitor_;
        std::string operation_;
        std::chrono::high_resolution_clock::time_point start_;
        bool enabled_;
      
    };
}
