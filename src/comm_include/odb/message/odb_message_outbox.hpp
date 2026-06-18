#pragma once
#include <memory>
#include <vector>
#include <sstream>
#include <odb/database.hxx>
#include <odb/transaction.hxx>
#include <odb/query.hxx>
#include <odb/result.hxx>
#include <odb/mysql/database.hxx>
#include "message_outbox.hxx"
#include "message_outbox-odb.hxx"
#include "../../comm.hpp"
#include "../../logger.hpp"

namespace odbMessage 
{
    using namespace messageSystem;
    
    /// @brief 消息事务表操作类
    class OdbMessageOutbox 
    {
    public:
        OdbMessageOutbox(const std::string& user,
                        const std::string& password,
                        const std::string& db,
                        const std::string& host,
                        int port)
        {
            _db = std::make_unique<odb::mysql::database>(
                user, password, db, host, port);
        }
        
        /// @brief 插入事务记录
        Response insert(MessageOutbox& outbox)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                _db->persist(outbox);
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("插入事务记录失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 批量插入事务记录
        Response batchInsert(std::vector<MessageOutbox>& outboxes)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                for (auto& outbox : outboxes)
                {
                    _db->persist(outbox);
                }
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("批量插入事务记录失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 更新事务记录
        Response update(const MessageOutbox& outbox)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                _db->update(outbox);
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("更新事务记录失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 删除事务记录
        Response erase(unsigned long long id)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                _db->erase<MessageOutbox>(id);
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("删除事务记录失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 根据消息ID删除事务记录
        Response eraseByMsgId(unsigned long long msg_id)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                odb::query<MessageOutbox> q(odb::query<MessageOutbox>::msg_id == msg_id);
                _db->erase_query<MessageOutbox>(q);
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("删除事务记录失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 获取待处理的事务记录
        std::vector<MessageOutbox> getPendingTasks(int limit = 100)
        {
            try 
            {
                odb::transaction t(_db->begin());
                unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                
                typedef odb::query<MessageOutbox> query;
                odb::result<MessageOutbox> r(
                    _db->query<MessageOutbox>(
                        query::status == static_cast<int>(OutboxStatus::PENDING) &&
                        query::next_retry_at <= now));
                
                std::vector<MessageOutbox> result;
                int count = 0;
                for (const auto& item : r)
                {
                    if (count >= limit) break;
                    result.push_back(item);
                    count++;
                }
                t.commit();
                return result;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("获取待处理事务记录失败:{}!", e.what());
                return {};
            }
        }
        
        /// @brief 更新事务状态为处理中
        Response markProcessing(unsigned long long id)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                std::stringstream ss;
                ss << "UPDATE message_outbox SET status=" << static_cast<int>(OutboxStatus::PROCESSING) 
                   << ", updated_at=" << now << " WHERE id=" << id << " AND status=" << static_cast<int>(OutboxStatus::PENDING);
                _db->execute(ss.str());
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("更新事务状态失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 更新事务状态为完成
        Response markCompleted(unsigned long long id)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                _db->erase<MessageOutbox>(id);
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("删除已完成事务记录失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 更新事务状态为失败
        Response markFailed(unsigned long long id, const std::string& error)
        {
            Response rep;
            try 
            {
                odb::transaction t(_db->begin());
                unsigned long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                
                odb::query<MessageOutbox> q(odb::query<MessageOutbox>::id == id);
                odb::result<MessageOutbox> r(_db->query<MessageOutbox>(q));
                auto it = r.begin();
                if (it == r.end())
                {
                    rep.status = false;
                    rep.errmsg = "事务记录不存在";
                    return rep;
                }
                MessageOutbox outbox(*it);
                outbox.retry_count++;
                outbox.last_error = error;
                outbox.updated_at = now;
                
                if (outbox.retry_count >= outbox.max_retries)
                {
                    outbox.status = static_cast<int>(OutboxStatus::FAILED);
                }
                else
                {
                    unsigned long long delay = (1ULL << outbox.retry_count) * 1000;
                    outbox.next_retry_at = outbox.updated_at + delay;
                    outbox.status = static_cast<int>(OutboxStatus::PENDING);
                }
                
                _db->update(outbox);
                t.commit();
                rep.status = true;
            }
            catch (const std::exception& e)
            {
                LOG_ERROR("更新事务状态失败:{}!", e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }

    private:
        std::unique_ptr<odb::mysql::database> _db;
    };
}
