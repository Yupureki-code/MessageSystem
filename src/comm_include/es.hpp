#pragma once
#include <elasticlient/client.h>
#include <cpr/response.h>
#include <json/json.h>
#include <json/value.h>
#include <json/writer.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "comm.hpp"
#include <sstream>

namespace messageSystem
{
    inline bool Serialize(const Json::Value &val, std::string* dst)
    {
        Json::StreamWriterBuilder swb;
        swb.settings_["emitUTF8"] = true;
        std::unique_ptr<Json::StreamWriter> sw(swb.newStreamWriter());
        std::stringstream ss;
        int ret = sw->write(val, &ss);
        if (ret != 0) 
        {
            LOG_ERROR("Json序列化失败!");
            return false;
        }
        *dst = ss.str();
        return true;
    }
    inline bool UnSerialize(const std::string &src, Json::Value *val)
    {
        Json::CharReaderBuilder crb;
        std::unique_ptr<Json::CharReader> cr(crb.newCharReader());
        std::string err;
        bool ret = cr->parse(src.c_str(), src.c_str() + src.size(), val, &err);
        if (ret == false) 
        {
            LOG_ERROR("json反序列化失败:{}!",err);
            return false;
        }
        return true;
    }

    inline std::shared_ptr<elasticlient::Client> es_client;
    inline bool InitESClient(const std::string& host)
    {
        es_client = std::make_shared<elasticlient::Client>(std::vector<std::string>{host});
        return true;
    }

    class ESIndex
    {
    public:
        ESIndex(std::shared_ptr<elasticlient::Client> client = es_client)
        :_client(client)
        {
            _settings["number_of_shards"] = 1;
            _settings["number_of_replicas"] = 0;
            _settings["analysis"]["analyzer"]["ik"]["tokenizer"] = "ik_max_word";
            _mappings["dynamic"] = true;
        }
        void setSettings(const std::string& analysis,int number_of_shards = 1,int number_of_replicas = 0)
        {
            _settings["analysis"]["analyzer"]["ik"]["tokenizer"] = analysis;
            _settings["number_of_shards"] = number_of_shards;
            _settings["number_of_replicas"] = number_of_replicas;
        }
        void setProperties(const std::string& name,const std::string& type,const std::string& analyzer = "ik_max_word")
        {
            _properties[name]["type"] = type;
            if(!analyzer.empty())
            {
                _properties[name]["analyzer"] = analyzer;
            }
        }
        bool create(const std::string& name)
        {
            if(!_properties.isNull())
            {
                _mappings["properties"] = _properties;
            }
            Json::Value json_out;
            json_out["settings"] = _settings;
            json_out["mappings"] = _mappings;
            std::string out;
            if(!Serialize(json_out,&out))
            {
                return false;
            }
            try 
            {
                auto rsp = _client->index(name, "", "", out);
                if (rsp.status_code < 200 || rsp.status_code >= 300) 
                {
                    LOG_ERROR("创建ES索引 {} 失败，响应状态码异常: {}", name, rsp.status_code);
                    return false;
                }
            } catch(std::exception &e) 
            {
                LOG_ERROR("创建ES索引 {} 失败: {}", name, e.what());
                return false;
            }
            return true;
        }
    private:
        std::shared_ptr<elasticlient::Client> _client;
        Json::Value _settings;
        Json::Value _mappings;
        Json::Value _properties;
    };

    class ESInsert
    {
    public:
        ESInsert(std::shared_ptr<elasticlient::Client> client = es_client)
        :_client(client)
        { }
        template<class T>
        ESInsert& add(const std::string& key,const T& value)
        {
            _insert[key] = value;
            return *this;
        }
        template<class T>
        ESInsert& operator+(const std::pair<std::string, T>& kv)
        {
            _insert[kv.first] = kv.second;
            return *this;
        }
        bool insert(const std::string& name,const std::string& type,const std::string& id)
        {
            std::string out;
            if(!Serialize(_insert,&out))
            {
                return false;
            }
            try 
            {
                auto rsp = _client->index(name, type, id, out);
                if (rsp.status_code < 200 || rsp.status_code >= 300) 
                {
                    LOG_ERROR("ES插入 {} 失败，响应状态码异常: {}", out, rsp.status_code);
                    return false;
                }
            } catch(std::exception &e) 
            {
                LOG_ERROR("ES插入 {} 失败: {}", out, e.what());
                return false;
            }
            return true;
        }
        bool batchInsert(const std::string& str)
        {
            try 
            {
                auto rsp = _client->performRequest(elasticlient::Client::HTTPMethod::POST,"_bulk", str);
                if (rsp.status_code < 200 || rsp.status_code >= 300) 
                {
                    LOG_ERROR("ES批量插入 {} 失败，响应状态码异常: {}", str, rsp.status_code);
                    return false;
                }
            } catch(std::exception &e) 
            {
                LOG_ERROR("ES批量插入 {} 失败: {}", str, e.what());
                return false;
            }
            return true;
        }
    private:
        std::shared_ptr<elasticlient::Client> _client;
        Json::Value _insert;
    };

    class ESRemove
    {
    public:
        ESRemove(std::shared_ptr<elasticlient::Client> client = es_client)
        :_client(client)
        { }
        bool remove(const std::string& name,const std::string& type,const std::string& id)
        {
            try 
            {
                auto rsp = _client->remove(name, type, id);
                if (rsp.status_code < 200 || rsp.status_code >= 300) 
                {
                    LOG_ERROR("删除数据 {} 失败，响应状态码异常: {}", id, rsp.status_code);
                    return false;
                }
            } 
            catch(std::exception &e) 
            {
                LOG_ERROR("删除数据 {} 失败: {}", id, e.what());
                return false;
            }
            return true;
        }
    private:
        std::shared_ptr<elasticlient::Client> _client;
    };

    class ESQuery
    {
    public:
        ESQuery(std::shared_ptr<elasticlient::Client> client = es_client)
        :_client(client)
        { }
        void addMust(const std::string& key,const std::string& value)
        {
            Json::Value match;
            match["match"][key] = value;
            _must.append(match);
        }
        void addMustNot(const std::string& key,const std::vector<std::string>& value)
        {
            Json::Value terms;
            for(auto & it : value)
            {
                terms["terms"][key].append(it);
            }
            _must_not.append(terms);
        }
        void addShould(const std::string& key,const std::string& value)
        {
            Json::Value match;
            match["match"][key] = value;
            _should.append(match);
        }
        void addFilter(const std::string& key,const std::vector<std::string>& value)
        {
            Json::Value terms;
            for(auto & it : value)
            {
                terms["terms"][key].append(it);
            }
            _filter.append(terms);
        }
        void addFilterTimeRange(const std::string& key,uint64_t start,uint64_t  end)
        {
            _filter["range"][key]["gte"] = start;
            _filter["range"][key]["lte"] = end;
        }
        bool query(const std::string& name,const std::string& type,Json::Value* value)
        {
            Json::Value body;
            if(!_must.empty()) body["must"] = _must;
            if(!_must_not.empty()) body["must_not"] = _must_not;
            if(!_should.empty()) body["should"] = _should;
            if(!_filter.empty()) body["filter"] = _filter;
            Json::Value json_out;
            json_out["query"]["bool"] = body;
            std::string out;
            if(!Serialize(json_out,&out))
            {
                return false;
            }
            std::string in;
            try 
            {
                auto rsp = _client->search(name, type, out);
                if (rsp.status_code < 200 || rsp.status_code >= 300) 
                {
                    LOG_ERROR("检索数据 {} 失败，响应状态码异常: {}", out, rsp.status_code);
                    return false;
                }
                in = rsp.text;
            } 
            catch(std::exception &e) 
            {
                LOG_ERROR("检索数据 {} 失败: {}", out, e.what());
                return false;
            }
            if(!UnSerialize(in,value))
            {
                return false;
            }
            return true;
        }
    private:
        std::shared_ptr<elasticlient::Client> _client;
        Json::Value _must;
        Json::Value _must_not;
        Json::Value _should;
        Json::Value _filter;
    };
}
