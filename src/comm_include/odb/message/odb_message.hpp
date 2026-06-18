#pragma once
#include "message-odb.hxx"
#include "../../comm.hpp"
#include "../../latecymonitor.hpp"
#include "message.hxx"
#include <odb/database.hxx>
#include <odb/transaction.hxx>
#include <odb/query.hxx>
#include <odb/result.hxx>
#include <odb/mysql/database.hxx>
#include <sstream>
#include <string>

namespace odbMessage
{
    using namespace messageSystem;
    using namespace latecyMonitor;
    
    /// @brief 消息数据库操作类
    class OdbMessage
    {
    public:
        typedef odb::query<Message> query;  
        
        OdbMessage(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        :_db(new odb::mysql::database(user,password,db,host,port))
        {
            _monitor.setOutputFile(LOG_PATH, "odb_message.log");
            _monitor.start();
        }
        
        /// @brief 插入单条消息
        Response insertMessage(Message& message)
        {
            Response rep;
            Timer t(_monitor,"insert message(id):" + std::to_string(message.id));
            try
            {
                odb::transaction t(_db->begin());
                _db->persist<Message>(message);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("插入消息{}失败:{}!",message.id,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 批量插入文本消息
        Response insertTextMessages(const std::vector<Message>& messages)
        {
            Response rep;
            Timer t(_monitor,"insert text messages(size):" + std::to_string(messages.size()));
            try
            {
                odb::transaction t(_db->begin());
                std::stringstream ss;
                ss<<"INSERT INTO message(message_id,conversation_id,sender_id,message_type,create_time,text) VALUES ";
                for(size_t i = 0; i < messages.size(); i++)
                {
                    if(i > 0) ss << ",";
                    ss << "('" << messages[i].message_id << "','" 
                       << messages[i].conversation_id << "','" 
                       << messages[i].sender_id << "'," 
                       << messages[i].message_type << "," 
                       << messages[i].create_time << ",'" 
                       << messages[i].text.get() << "')";
                }
                _db->execute(ss.str());
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("插入一批文本消息失败:{}!",e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 批量插入文件消息
        Response insertFileMessages(const std::vector<Message>& messages)
        {
            Response rep;
            Timer t(_monitor,"insert file messages(size):" + std::to_string(messages.size()));
            try
            {
                odb::transaction t(_db->begin());
                std::stringstream ss;
                ss<<"INSERT INTO message(message_id,conversation_id,sender_id,message_type,create_time,file_id,file_name,file_size) VALUES ";
                for(size_t i = 0; i < messages.size(); i++)
                {
                    if(i > 0) ss << ",";
                    ss << "('" << messages[i].message_id << "','" 
                       << messages[i].conversation_id << "','" 
                       << messages[i].sender_id << "'," 
                       << messages[i].message_type << "," 
                       << messages[i].create_time << ",'" 
                       << messages[i].file_id.get() << "','" 
                       << messages[i].file_name.get() << "'," 
                       << messages[i].file_size.get() << ")";
                }
                _db->execute(ss.str());
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("插入一批文件消息失败:{}!",e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 删除消息
        Response deleteMessage(const std::string& message_id)
        {
            Response rep;
            Timer t(_monitor,"delete message(mid):" + message_id);
            try
            {
                odb::transaction t(_db->begin());
                _db->erase_query<Message>(query::message_id == message_id);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("删除消息{}失败:{}!",message_id,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 删除会话所有消息
        Response removeConversation(const std::string& id)
        {
            Response rep;
            Timer t(_monitor,"remove conversation(cid):" + id);
            try
            {
                odb::transaction t(_db->begin());
                _db->erase_query<Message>(query::conversation_id == id);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("删除会话{}失败:{}!",id,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 获取最近消息
        Response getRecentMessages(const std::string& id, int count, std::vector<Message>* messages)
        {
            Response rep;
            Timer t(_monitor,"get recent messages(cid,count):" + id + ":" + std::to_string(count));
            try
            {
                odb::transaction t(_db->begin());
                std::stringstream cond;
                cond << "conversation_id='" << id << "' ";
                cond << "order by create_time desc limit " << count;
                auto ret = _db->query<Message>(cond.str());
                for(auto & it : ret)
                {
                    messages->emplace_back(it);
                }
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("获取最近会话{}消息{}失败:{}!",id,count,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        
        /// @brief 获取历史消息
        Response getHistoryMessages(const std::string& id, unsigned long long start, unsigned long long end, std::vector<Message>* messages)
        {
            Response rep;
            Timer t(_monitor,"get history messages(cid,start,end):" + id + " " + 
                std::to_string(start) + "-" + std::to_string(end));
            try
            {
                odb::transaction t(_db->begin());
                auto ret = _db->query<Message>(
                    query::conversation_id == id && 
                    query::create_time >= start && 
                    query::create_time <= end);
                for(auto & it : ret)
                {
                    messages->emplace_back(it);
                }
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("获取历史会话{}消息{}-{}失败:{}!",id, start, end, e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }

    private:
        std::unique_ptr<odb::database> _db;
        latecyMonitor::LatencyMonitor _monitor;
    };
}
