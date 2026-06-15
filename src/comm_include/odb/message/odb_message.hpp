#include "message-odb.hxx"
#include "../../comm.hpp"
#include "../../latecymonitor.hpp"
#include "message.hxx"
// ODB 核心头文件
#include <odb/database.hxx>      // database 基类
#include <odb/transaction.hxx>   // 事务管理
#include <odb/query.hxx>         // 类型安全查询
#include <odb/result.hxx>        // 查询结果集
// MySQL 数据库驱动
#include <odb/mysql/database.hxx>
#include <sstream>
#include <string>

namespace odbMessage
{
    using namespace messageSystem;
    using namespace latecyMonitor;
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
            _monitor.setOutputFile(LOG_PATH, "odb_user.log");
            _monitor.start();
        }
        bool insertMessage(const Message& message)
        {
            Timer t(_monitor,"insert message(id):" + std::to_string(message.id));
            try
            {
                odb::transaction t(_db->begin());
                _db->persist<Message>(message);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("插入消息{}失败:{}!",message.id,e.what());
                return false;
            }
            return true;
        }
        bool removeConversation(const std::string& id)
        {
            Timer t(_monitor,"remove conversation(cid):" + id);
            try
            {
                odb::transaction t(_db->begin());
                _db->erase_query<Message>(query::conversation_id == id);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("删除会话{}失败:{}!",id,e.what());
                return false;
            }
            return true;
        }
        bool getRecentMessages(const std::string& id,int count,std::vector<Message>* messages)
        {
            Timer t(_monitor,"get current messages(cid,count):" + id + ":" + std::to_string(count));
            try
            {
                odb::transaction t(_db->begin());
                std::stringstream cond;
                cond << "session_id='" << id << "' ";
                cond << "order by create_time desc limit " << count;
                auto ret = _db->query<Message>(cond.str());
                for(auto & it : ret)
                {
                    messages->emplace_back(it);
                }
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("获取最近会话{}消息{}失败:{}!",id,count,e.what());
                return false;
            }
            return true;
        }
        bool getHistoryMessages(const std::string& id,boost::posix_time::ptime &start,boost::posix_time::ptime &end,std::vector<Message>* messages)
        {
            Timer t(_monitor,"get range messages(cid,start,end):" + id + " " + 
                boost::posix_time::to_simple_string(start) + "-" + boost::posix_time::to_simple_string(end));
            try
            {
                odb::transaction t(_db->begin());
                auto ret = _db->query<Message>(query::conversation_id == id && query::create_time >= start && query::create_time <= end);
                for(auto & it : ret)
                {
                    messages->emplace_back(it);
                }
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("获取历史会话{}消息{}-{}失败:{}!",id, 
                    boost::posix_time::to_simple_string(start), 
                    boost::posix_time::to_simple_string(end),e.what());
                return false;
            }
            return true;
        }
    private:
        std::unique_ptr<odb::database> _db;
        latecyMonitor::LatencyMonitor _monitor;
    };
};
