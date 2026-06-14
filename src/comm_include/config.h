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

inline int GetEnvOrDefault(const char* key, int fallback)
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
const std::string LOG_PATH = "/home/yupureki/project/MessageSystem/data/";

const std::string smtp_host = GetEnvOrDefault("OJ_SMTP_HOST", "");
const int smtp_port = GetEnvOrDefault("OJ_SMTP_PORT", ParseIntOrDefault("", 465));
const std::string smtp_user = GetEnvOrDefault("OJ_SMTP_USER", "");
const std::string smtp_passwd = GetEnvOrDefault("OJ_SMTP_PASS", "");
const std::string smtp_from = GetEnvOrDefault("OJ_SMTP_FROM", "");
const bool smtp_ssl = GetEnvOrDefault("OJ_SMTP_SSL", "") == "true";
