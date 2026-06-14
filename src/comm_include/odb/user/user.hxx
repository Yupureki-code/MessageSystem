#pragma once
#include <string>
#include <cstddef> 
#include <odb/nullable.hxx>
#include <odb/core.hxx>

enum Sex
{
    male,
    female,
    unknown
};

#pragma db object table("user")
struct User
{
    #pragma db id auto
    size_t uid;
    #pragma db type("varchar(20)") unique not_null
    std::string name;
    #pragma db type("varchar(40)") not_null
    std::string password;
    Sex sex;
    #pragma db type("bit(8)")
    size_t age;
    #pragma db type("varchar(30)") unique not_null
    std::string email;
    std::string desc;
    std::string avatar;
};