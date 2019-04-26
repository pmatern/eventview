cmake_minimum_required(VERSION 3.14)
project(eventview)

set(CMAKE_CXX_STANDARD 17)

add_library(eventview eventview.cpp eventview.h snowflake.h eventwriter.h eventlog.h entitystorage.h publish.h view.h)

add_executable(eventview_tests tests.cpp catch.h eventview.h snowflake.h eventwriter.h eventlog.h entitystorage.h publish.h view.h)