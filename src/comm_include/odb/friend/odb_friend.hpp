#include "friend-odb.hxx"
#include "friend.hxx"
#include <memory>
#include <odb/exception.hxx>
#include <odb/forward.hxx>
#include <vector>
#include "../../logger.hpp"
// ODB 核心头文件
#include <odb/database.hxx>      // database 基类
#include <odb/transaction.hxx>   // 事务管理
#include <odb/query.hxx>         // 类型安全查询
#include <odb/result.hxx>        // 查询结果集
// MySQL 数据库驱动
#include <odb/mysql/database.hxx>
#include <sstream>
#include "../../latecymonitor.hpp"
#include "../../comm.hpp"

namespace odbFriend
{
    using namespace messageSystem;
    using latecyMonitor::Timer;
    class OdbFriend
    {
    public:
        typedef odb::query<Friendships> query;  
        OdbFriend(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        :_db(new odb::mysql::database(user,password,db,host,port))
        {
            _monitor.setOutputFile(LOG_PATH, "odb_user.log");
            _monitor.start();
        }
        bool insert(Friendships& table)
        {
            Timer t(_monitor,"insert friendships(id):" + std::to_string(table.id));
            try
            {
                odb::transaction t(_db->begin());
                _db->persist<Friendships>(table);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("插入好友关系表{}失败:{}!",table.id,e.what());
                return false;
            }
            return true;
        }
        bool remove(const Friendships& table)
        {
            Timer t(_monitor,"remove friendships(id):" + std::to_string(table.id));
            try
            {
                odb::transaction t(_db->begin());
                _db->erase(table);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("删除好友关系表{}失败:{}!",table.id,e.what());
                return false;
            }
            return true;
        }
        bool selectById(size_t id,std::shared_ptr<Friendships>* table)
        {
            Timer t(_monitor,"select friendships(id):" + std::to_string(id));
            try
            {
                odb::transaction t(_db->begin());
                auto ret = _db->query_one<Friendships>(query::id == id && query::status == FriendShipStatus::APPROVAL);
                table->reset(ret);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("查找好友关系表{}失败:{}!",id,e.what());
                return false;
            }
            return true;
        }
        bool selectByUid(size_t uid,size_t friend_uid,std::shared_ptr<Friendships>* table)
        {
            Timer t(_monitor,"select friendships(uid,friend_uid):" + std::to_string(uid) + "," + std::to_string(friend_uid));
            try
            {
                odb::transaction t(_db->begin());
                auto ret = _db->query_one<Friendships>(query::uid == uid && query::friend_uid == friend_uid && query::status == FriendShipStatus::APPROVAL);
                table->reset(ret);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("查找好友关系表{}:{}失败:{}!",uid,friend_uid,e.what());
                return false;
            }
            return true;
        }
        bool update(const Friendships& table)
        {
            Timer t(_monitor,"update friendships(id):" + std::to_string(table.id));
            try
            {
                odb::transaction t(_db->begin());
                _db->update(table);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("查找好友关系表{}失败:{}!",table.id,e.what());
                return false;
            }
            return true;
        }
        odb::database* getDB() const { return _db.get(); }
        /// @brief 通过好友名称模糊搜索好友（使用FindFriend视图）
        bool selectFriendsByName(size_t uid, const std::string& name, std::vector<FindFriend>* result)
        {
            Timer t(_monitor,"select friends by name(uid,name):" + std::to_string(uid) + "," + name);
            try
            {
                odb::transaction t(_db->begin());
                // 使用原生SQL查询FindFriend视图
                std::string sql = "SELECT * FROM friendships "
                    "INNER JOIN user ON user.uid = friendships.friend_uid "
                    "WHERE friendships.uid = " + std::to_string(uid) +
                    " AND friendships.status = 'APPROVAL'";
                if (!name.empty())
                {
                    sql += " AND user.name LIKE '%" + name + "%'";
                }
                odb::result<FindFriend> r(_db->query<FindFriend>(sql));
                for (auto it = r.begin(); it != r.end(); ++it)
                {
                    result->push_back(*it);
                }
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("搜索好友失败(uid,name):{}:{}:{}!",uid,name,e.what());
                return false;
            }
            return true;
        }
    private:
        std::unique_ptr<odb::database> _db;
        latecyMonitor::LatencyMonitor _monitor;
    };
}