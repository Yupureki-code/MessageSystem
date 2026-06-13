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

const std::string DATA_PATH = "/home/yupureki/project/MessageSystem/data/";
