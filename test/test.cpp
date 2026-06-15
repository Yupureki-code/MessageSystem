#include <json/json.h>
#include <json/value.h>
#include <json/writer.h>
#include <sstream>
#include <iostream>
#include <memory>

int main()
{
    Json::Value test;
    test["test1"] = "test1";
    test["test2"]["test3"] = "test2";
    Json::Value test2;
    test2["test4"] = "test4";
    test["test6"] = test2;
    Json::StreamWriterBuilder sb;
    std::unique_ptr<Json::StreamWriter> sw(sb.newStreamWriter());
    std::stringstream ss;
    sw->write(test, &ss);
    std::cout<<ss.str()<<std::endl;
}