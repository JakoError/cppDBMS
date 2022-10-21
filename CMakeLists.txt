cmake_minimum_required(VERSION 3.23)
project(DBMS)

set(CMAKE_CXX_STANDARD 20)

set(BOOST_INC_DIR  D:/Codes/C++/boost_1_80_0)
set(BOOST_LINK_DIR  D:/Codes/C++/boost_1_80_0/stage/lib)

include_directories(${BOOST_INC_DIR})
link_directories(${BOOST_LINK_DIR})

add_executable(cppDBMS main.cpp)