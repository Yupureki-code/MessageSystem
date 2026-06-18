#include "../../comm_include/proto_include/conversation.pb.h"
#include "../../comm_include/odb/conversation/odb_conversation.hpp"
#include "../../comm_include/odb/conversation/conversation.hxx"
#include "../../comm_include/es.hpp"
#include <future>
#include <json/value.h>
#include <ratio>
#include <vector>


namespace messageSystem
{
    using namespace odbConversation;
    class ConversationDB
    {
    private:
        void praseFromSerachForConversationByES(Json::Value value,std::vector<Conversation>& v)
        {
            if(value.isMember("hits") && value["hits"].isMember("hits") && value["hits"]["hits"].isArray())
            {
                auto array = value["hits"]["hits"];
                for(int i = 0;i<array.size();i++)
                {
                    Conversation c;
                    c.conversation_id = array[i]["conversation_id"].asInt();
                    c.name = array[i]["name"].asString();
                    c.avatar = array[i]["avatar"].asString();
                    v.emplace_back(c);
                }
            }
        }
    public:
        ConversationDB(const std::string& user
            ,const std::string& password
            ,const std::string& db
            ,const std::string& host
            ,int port)
        :_odb(user,password,db,host,port)
        {}
        Response insertConversation(const Conversation& table,size_t * id,std::vector<ConversationMember>& members)
        {
            Response rep = _odb.insertConversation(table,id);
            if(!rep.status)
                return rep;
            for(int i = 0;i<members.size();i++)
            {
                members[i].conversation_id = *id;
                _odb.addConversationMember(members[i]);
            }
            if(table.type == ConversationType::GROUP)
            {
                ESInsert es;
                es
                .add("conversaion_id", table.conversation_id)
                .add("name",table.name)
                .add("avatar",table.avatar);
                rep = es.insert("conversation", "_doc", std::to_string(table.conversation_id));
                if(!rep.status)
                {
                    rep.status = false;
                    rep.errmsg = "插入ES失败!";
                }
            }
            return rep;
        }
        Response removeConversation(const std::string& cid,const std::string& owner_id)
        {
            std::shared_ptr<ConversationMember> member;
            auto ret = _odb.getCoversationMember(owner_id, cid, &member);
            if(ret.status == false)return ret;
            if(member->power != ConversationMemberPower::OWNER)
            {
                ret.status = false;
                ret.errmsg = "该用户不是群主!";
                return ret;
            }
            ret = _odb.removeConversation(cid);
            return ret;
        }
        Response addMember(const ConversationMember member)
        {
            auto rep = _odb.addConversationMember(member);
            if(!rep.status) return rep;
            ESInsert es;
            es
            .add("conversation_id", member.conversation_id)
            .add("uid", member.uid)
            .add("conversation_member_name", member.conversation_member_name)
            .add("conversation_remark_name",member.conversation_remark_name)
            .add("power",member.power);
            rep = es.insert("conversation", "_doc", std::to_string(member.conversation_id) + std::to_string(member.uid));
            return rep;
        }
        Response getPrivateConversations(const std::string& uid,std::vector<PrivateConversation>* conversations)
        {
            return _odb.getPrivateConversations(uid, conversations);
        }
        Response getGroupConversations(const std::string& uid,std::vector<GroupConversation>* conversations)
        {
            return _odb.getGroupConversations(uid, conversations);
        }
        Response exitConversation(const std::string& cid,const std::string& uid)
        {
            auto rep = _odb.removeConversationMember(cid, uid);
            if(!rep.status)return rep;
            ESRemove es;
            rep = es.remove("conversation", "_doc", cid + uid);
            return rep;
        }
        Response changeMemberPower(const std::string& cid,const std::string& uid,const std::string owner_id,int power)
        {
            std::shared_ptr<ConversationMember> member;
            auto ret = _odb.getCoversationMember(owner_id, cid, &member);
            if(ret.status == false)return ret;
            if(member->power != ConversationMemberPower::OWNER)
            {
                ret.status = false;
                ret.errmsg = "该用户不是群主!";
                return ret;
            }
            return _odb.changeMemberPower(cid, uid, owner_id, power);
        }
        Response SearchConversation(const std::string& cid,const std::string& name,std::vector<Conversation>* conversations)
        {
            Response rep;
            {
                ESQuery es;
                es.addMust("conversation_id", cid);
                Json::Value value;
                rep = es.query("conversation", "_search", &value);
                if(!rep.status)return rep;
                praseFromSerachForConversationByES(value, *conversations);
            }
            {
                ESQuery es;
                es.addShould("name", name);
                Json::Value value;
                rep = es.query("conversation", "_search", &value);
                if(!rep.status)return rep;
                praseFromSerachForConversationByES(value, *conversations);
            }
            return rep;
        }
        Response getConversationMemberList(const std::string& cid,std::vector<UserInfo>* users)
        {
            Response rep;
            ESQuery es;
            es.addMust("conversation_id", cid);
            Json::Value value;
            rep = es.query("conversation", "_search", &value);
            if(value.isMember("hits") && value["hits"].isMember("hits") && value["hits"]["hits"].isArray())
            {
                auto array = value["hits"]["hits"];
                for(int i = 0;i<array.size();i++)
                {
                    UserInfo user;
                    user.set_user_id(array[i].asString());
                    user.set_nickname(array[i]["conversation_member_name"].asString());
                    users->emplace_back(user);
                }
            }
            return rep;
        }
    private:
        OdbConversation _odb;
        
    };
};