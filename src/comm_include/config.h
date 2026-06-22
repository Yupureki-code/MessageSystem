#include <string>

inline std::string GetEnvOrDefault(const char* key, const std::string& fallback)
{
    const char* value = std::getenv(key);
    if (value == nullptr)
    {
        return fallback;
    }
    std::string out(value);
    if (out.empty())
    {
        return fallback;
    }
    return out;
}

inline int GetEnvIntOrDefault(const char* key, int fallback)
{
    const char* value = std::getenv(key);
    if (value == nullptr)
    {
        return fallback;
    }
    try
    {
        return std::stoi(value);
    }
    catch (...)
    {
        return fallback;
    }
}

inline int ParseIntOrDefault(const std::string& value, int fallback)
{
    if (value.empty())
    {
        return fallback;
    }
    try
    {
        return std::stoi(value);
    }
    catch (...)
    {
        return fallback;
    }
}

const std::string DATA_PATH = "/home/yupureki/project/MessageSystem/data/";
const std::string LOG_PATH = "/home/yupureki/project/MessageSystem/log/";

const std::string SMTP_HOST = GetEnvOrDefault("IM_SMTP_HOST", "");
const int SMTP_PORT = GetEnvIntOrDefault("IM_SMTP_PORT", ParseIntOrDefault("", 465));
const std::string SMTP_USER = GetEnvOrDefault("IM_SMTP_USER", "");
const std::string SMTP_PASSWD = GetEnvOrDefault("IM_SMTP_PASS", "");
const std::string SMTP_FROM = GetEnvOrDefault("IM_SMTP_FROM", "");
const bool SMTP_SSL = GetEnvOrDefault("IM_SMTP_SSL", "") == "true";

const std::string MYSQL_HOST = GetEnvOrDefault("IM_MYSQL_HOST", "");
const std::string MYSQL_USER = GetEnvOrDefault("IM_MYSQL_USER", "");
const std::string MYSQL_PASSWD = GetEnvOrDefault("IM_MYSQL_PASSWD", "");
const std::string MYSQL_DB = GetEnvOrDefault("IM_MYSQL_DB", "");
const int MYSQL_SERVER_PORT = GetEnvIntOrDefault("IM_MYSQL_PORT", ParseIntOrDefault("", 3306));

const std::string REDIS_HOST = GetEnvOrDefault("IM_REDIS_HOST", "");
const int REDIS_PORT = GetEnvIntOrDefault("IM_REDIS_PORT", ParseIntOrDefault("", 6379));

const std::string ES_HOST = GetEnvOrDefault("IM_ES_HOST", "");
