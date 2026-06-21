#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <iostream>
using json = nlohmann::json;

int main()
{
    json test;
    test["string"] = "test";
    test["int"] = 1;
    test["float"] = 1.11;
    test["array"] = {"test1"};
    test["array"].push_back("test2");
    test["1"]["2"]["3"] = "test3";
    std::string str = test.dump(4);
    std::cout<<str<<std::endl;
}