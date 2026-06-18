#include "conversation-odb.hxx"
#include "conversation.hxx"
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
#include "../../es.hpp"

namespace odbConversation
{
    using namespace messageSystem;
    using latecyMonitor::Timer;
    class OdbConversation
    {
    public:
        typedef odb::query<Conversation> query;  
        OdbConversation(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        :_db(new odb::mysql::database(user,password,db,host,port))
        {
            _monitor.setOutputFile(LOG_PATH, "odb_user.log");
            _monitor.start();
        }
        Response insertConversation(const Conversation& table,size_t* id)
        {
            Response rep;
            Timer t(_monitor,"insert conversaion(id):" + std::to_string(table.conversation_id));
            try
            {
                odb::transaction t(_db->begin());
                *id = _db->persist<Conversation>(table);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        Response removeConversation(const std::string& id)
        {
            Response rep;
            Timer t(_monitor,"remove conversaion(id):" + id);
            try
            {
                size_t id_int = stoi(id);
                odb::transaction t(_db->begin());
                _db->erase<Conversation>(id_int);
                _db->erase<ConversationMember>(id_int);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        Response getCoversationMember(const std::string uid,const std::string cid,std::shared_ptr<ConversationMember>* member)
        {
            Response rep;
            Timer t(_monitor,"get conversaion_member(cid,uid):" + cid + ":" + uid);
            try
            {
                typedef odb::query<ConversationMember> query; 
                odb::transaction t(_db->begin());
                auto ret = _db->load<ConversationMember>(stoi(cid + uid));
                member->reset(ret);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        Response getPrivateConversations(const std::string& uid,std::vector<PrivateConversation>* conversations)
        {
            Response rep;
            Timer t(_monitor,"get private_conversaions(id):" + uid);
            try
            {
                odb::transaction t(_db->begin());
                // 查询私聊会话：类型为PRIVATE且用户是成员
                std::string sql = "SELECT Conversation.conversation_id, user.name, user.avatar "
                    "FROM Conversation "
                    "INNER JOIN Conversation_member ON Conversation_member.conversation_id = Conversation.conversation_id "
                    "INNER JOIN user ON user.uid != Conversation_member.uid "
                    "WHERE Conversation.type = 'PRIVATE' AND Conversation_member.uid = " + uid;
                odb::result<PrivateConversation> r(_db->query<PrivateConversation>(sql));
                for (auto it = r.begin(); it != r.end(); ++it)
                {
                    conversations->push_back(*it);
                }
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        Response getGroupConversations(const std::string& uid,std::vector<GroupConversation>* conversations)
        {
            Response rep;
            Timer t(_monitor,"get group_conversaions(id):" + uid);
            try
            {
                odb::transaction t(_db->begin());
                // 查询群聊会话：类型为GROUP且用户是成员
                std::string sql = "SELECT Conversation.conversation_id, Conversation_member.conversation_remark_name, "
                    "Conversation.avatar, Conversation.members "
                    "FROM Conversation "
                    "INNER JOIN Conversation_member ON Conversation_member.conversation_id = Conversation.conversation_id "
                    "WHERE Conversation.type = 'GROUP' AND Conversation_member.uid = " + uid;
                odb::result<GroupConversation> r(_db->query<GroupConversation>(sql));
                for (auto it = r.begin(); it != r.end(); ++it)
                {
                    conversations->push_back(*it);
                }
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                rep.status = false;
                rep.errmsg = e.what();
                return rep;
            }
            return rep;
        }
        bool updateConversationName(const std::string&id,const std::string& name)
        {
            Timer t(_monitor,"update conversaion_name(id):" + id);
            try
            {
                odb::transaction t(_db->begin());
                auto ret = _db->load<Conversation>(std::stoi(id));
                ret->name = name;
                _db->update(ret);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("更新群名称{}失败:{}!",id,e.what());
                return false;
            }
            return true;
        }
        bool updateConversationAvatar(const std::string&id,const std::string& avatar)
        {
            Timer t(_monitor,"update conversaion_avatar(id):" + id);
            try
            {
                odb::transaction t(_db->begin());
                auto ret = _db->load<Conversation>(std::stoi(id));
                ret->avatar = avatar;
                _db->update(ret);
                t.commit();
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("更新群头像{}失败:{}!",id,e.what());
                return false;
            }
            return true;
        }
        Response addConversationMember(const ConversationMember& member)
        {
            Response rep;
            std::string id = std::to_string(member.conversation_id);
            Timer t(_monitor,"add conversaion_member(id):" + id);
            try
            {
                odb::transaction t(_db->begin());
                _db->persist<ConversationMember>(member);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                rep.errmsg = e.what();
                rep.status = false;
                return rep;
            }
            return rep;
        }
        Response removeConversationMember(const std::string& cid,const std::string& uid)
        {
            Response rep;
            Timer t(_monitor,"remove conversaion_member(cid,uid):" + cid + ":" + uid);
            try
            {
                typedef odb::query<ConversationMember> query;  
                odb::transaction t(_db->begin());
                _db->erase_query<ConversationMember>(query::conversation_id == stoi(cid) && query::uid == stoi(uid));
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                rep.errmsg = e.what();
                rep.status = false;
                return rep;
            }
            return rep;
        }
        Response changeMemberPower(const std::string& cid,const std::string& uid,const std::string owner_id,int power)
        {
            Response rep;
            Timer t(_monitor,"remove conversaion_member(cid,uid):" + cid + ":" + uid);
            try
            {
                typedef odb::query<ConversationMember> query;  
                odb::transaction t(_db->begin());
                auto ret = _db->load<ConversationMember>(stoi(cid + uid));
                ret->power = static_cast<ConversationMemberPower>(power);
                _db->persist(ret);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                rep.errmsg = e.what();
                rep.status = false;
                return rep;
            }
            return rep;
        }
    private:
        std::unique_ptr<odb::database> _db;
        latecyMonitor::LatencyMonitor _monitor;
    };
}