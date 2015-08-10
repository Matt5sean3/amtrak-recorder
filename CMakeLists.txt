cmake_minimum_required(VERSION 2.8)
project(train_alarm)

find_package(arclaunch REQUIRED)

set(HOME_DIRECTORY "$ENV{HOME}")
configure_file(xml/launch.xml.in ${CMAKE_CURRENT_BINARY_DIR}/xml/launch.xml)
configure_file(xml/only_record.xml.in ${CMAKE_CURRENT_BINARY_DIR}/xml/only_record.xml)

