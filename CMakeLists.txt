cmake_minimum_required(VERSION 3.10)
project(MRpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost COMPONENTS mpi REQUIRED)

add_subdirectory(src)
include_directories(include ${Boost_INCLUDE_DIRS})

add_executable(MRpp main.cpp)
target_link_libraries(MRpp MRppLib ${Boost_LIBRARIES})
