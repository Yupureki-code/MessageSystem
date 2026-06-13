cmake_minimum_required(VERSION 3.28.3)

project(MessageSystem LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

set(DATA_PATH ${CMAKE_SOURCE_DIR}/data CACHE STRING "default conf file path")

configure_file(${CMAKE_SOURCE_DIR}/config.in.h ${CMAKE_SOURCE_DIR}/src/comm_include/config.h)