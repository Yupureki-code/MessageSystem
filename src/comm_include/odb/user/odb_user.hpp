#pragma once
#include "user-odb.hxx"
#include "user.hxx"
#include <memory>
#include <odb/exception.hxx>
#include <odb/forward.hxx>
#include <vector>
#include "../../logger.hpp"
#include <odb/database.hxx>
#include <odb/transaction.hxx>
#include <odb/query.hxx>
#include <odb/result.hxx>
#include <odb/mysql/database.hxx>
#include <sstream>
#include "../../latecymonitor.hpp"
#include "../../comm.hpp"

namespace odbUser
{
    using namespace messageSystem;
    using latecyMonitor::Timer;
    class OdbUser
    {
    public:  
        typedef odb::query<User> query;  
        OdbUser(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        :_db(new odb::mysql::database(user,password,db,host,port))
        {
            _monitor.setOutputFile(LOG_PATH, "odb_user.log");
            _monitor.start();
        }
        Response insert(User& user)
        {
            Response rep;
            Timer t(_monitor,"insert user(id):" + std::to_string(user.uid));
            try
            {
                odb::transaction t(_db->begin());
                _db->persist<User>(user);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("插入{}失败:{}!",user.uid,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        Response selectById(const std::string& id,std::shared_ptr<User>* user)
        {
            Response rep;
            Timer t(_monitor,"select user(id):" +  id);
            try
            {   
                odb::transaction t(_db->begin());
                auto ret = _db->query_one<User>(query::uid == stoi(id));
                user->reset(ret);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("查找{}失败:{}!",id,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        Response selectByName(const std::string name,std::shared_ptr<User>* user)
        {
            Response rep;
            Timer t(_monitor,"select user(name):" + name);
            try
            {   
                odb::transaction t(_db->begin());
                auto ret = _db->query_one<User>(query::name == name);
                user->reset(ret);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("查找用户{}失败:{}!",name,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        Response selectByEmail(const std::string email,std::shared_ptr<User>* user)
        {
            Response rep;
            Timer t(_monitor,"select user(email):" + email);
            try
            {   
                odb::transaction t(_db->begin());
                auto ret = _db->query_one<User>(query::name == email);
                user->reset(ret);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("查找用户{}失败:{}!",email,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        Response update(const std::shared_ptr<User>& user)
        {
            Response rep;
            Timer t(_monitor,"update user(id):" + std::to_string(user->uid));
            try
            {
                odb::transaction t(_db->begin());
                _db->update<User>(*user);
                t.commit();
                rep.status = true;
            }
            catch(const odb::exception& e)
            {
                LOG_ERROR("修改用户{}失败:{}!",user->uid,e.what());
                rep.status = false;
                rep.errmsg = e.what();
            }
            return rep;
        }
        std::vector<User> selectMultiById(const std::vector<std::string>& list)
        {
            std::vector<User> res;
            std::stringstream ss;
            for (const auto &id : list) 
            {
                 ss << "'" << id << "',";
            }
            std::string condition = ss.str();
            condition.pop_back();
            condition += ")";
            Timer t(_monitor,"select users(id):" +condition);
            try 
            {
                odb::transaction trans(_db->begin());
                typedef odb::query<User> query;
                typedef odb::result<User> result;
                result r(_db->query<User>(condition));
                for (result::iterator i(r.begin()); i != r.end(); ++i) 
                {
                    res.push_back(*i);
                }
                trans.commit();
            }
            catch (std::exception &e) 
            {
                LOG_ERROR("批量用户ID查询失败:{}!", e.what());
            }
            return res;
        }
    private:
        std::unique_ptr<odb::database> _db;
        latecyMonitor::LatencyMonitor _monitor;
    };
}
