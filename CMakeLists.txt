cmake_minimum_required(VERSION 3.14)
project(eventview)

set(CMAKE_CXX_STANDARD 17)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -O1 -fno-omit-frame-pointer -g")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -O1 -fno-omit-frame-pointer -g")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -O1 -fno-omit-frame-pointer -g")

add_library(eventview types.cpp types.h snowflake.h eventwriter.h eventlog.h entitystorage.h publish.h view.h mpsc.h opdispatch.h viewimpl.h publishimpl.h)

add_executable(eventview_tests tests.cpp catch.h types.h snowflake.h eventwriter.h eventlog.h entitystorage.h publish.h view.h mpsc.h opdispatch.h viewimpl.h publishimpl.h)
