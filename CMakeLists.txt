cmake_minimum_required(VERSION 2.8)
project(train_alarm)

find_package(arclaunch REQUIRED)

set(GATE_KEEPER_USERNAME "train_db_gate" CACHE STRING "User name for creator of user accounts")
set(GATE_KEEPER_USERHOST "localhost" CACHE STRING "Web address of script for creating user accounts")
set(GATE_KEEPER_USER "'${GATE_KEEPER_USERNAME}'@'${GATE_KEEPER_USERHOST}'")
string(RANDOM LENGTH 20 PASSWD)
set(GATE_KEEPER_PASSWD "${PASSWD}" CACHE STRING "Password for creator of user accounts")

set(RECORDER_USERNAME "train_db_rec" CACHE STRING "User name for recorder of basic train information")
set(RECORDER_USERHOST "localhost" CACHE STRING "Web address of script pulling basic train information")
set(RECORDER_USER "'${RECORDER_USERNAME}'@'${RECORDER_USERHOST}'")
string(RANDOM LENGTH 20 PASSWD)
set(RECORDER_PASSWD "${PASSWD}" CACHE STRING "Password for recorder of basic train information")

set(ANALYZER_USERNAME "train_db_ana" CACHE STRING "User name for predicter of train arrivals and departures")
set(ANALYZER_USERHOST "localhost" CACHE STRING "Web address of script predicting train arrivals and departures")
set(ANALYZER_USER "'${ANALYZER_USERNAME}'@'${ANALYZER_USERHOST}'")
string(RANDOM LENGTH 20 PASSWD)
set(ANALYZER_PASSWD "${PASSWD}" CACHE STRING "Password for predicter of train arrivals and departures")

set(TRAIN_DATABASE "train_db" CACHE STRING "Name of the train information database")
set(ALIAS_TABLE "alias" CACHE STRING "Name of the alias table in the train information database")
set(STATION_TABLE "station" CACHE STRING "Name of the station table in the train information database")
set(STOP_TABLE "stop" CACHE STRING "Name of the stop table in the train information database")
set(TRAIN_TABLE "train" CACHE STRING "Name of the train table in the train information database")

set(ARRIVAL_TABLE "arrival" CACHE STRING "Name of the arrival table in the prediction database")
set(DEPARTURE_TABLE "departure" CACHE STRING "Name of the departure table in the prediction database")

set(USER_DATABASE "train_db_user" CACHE STRING "Name of the user database")

set(TRAIN_DB_HOST "localhost" CACHE STRING "Web address of the server hosting the database")
set(TRAIN_DB_PORT "3306")
set(TRAIN_DB_WEB_HOST "localhost" CACHE STRING "Web address of the server hosting the application")

set(HOME_DIRECTORY "$ENV{HOME}")
set(PYTHON_PATH "${CMAKE_CURRENT_BINARY_DIR}/scripts/mod")

set(JSON_CONF_PATH "${CMAKE_CURRENT_BINARY_DIR}/config/database.json")

file(WRITE scripts/cgi/conf_path ${JSON_CONF_PATH})
configure_file(xml/launch.xml.in ${CMAKE_CURRENT_BINARY_DIR}/xml/launch.xml)
configure_file(xml/only_record.xml.in ${CMAKE_CURRENT_BINARY_DIR}/xml/only_record.xml)
configure_file(config/drop.mysql.in ${CMAKE_CURRENT_BINARY_DIR}/config/drop.mysql)
configure_file(config/create.mysql.in ${CMAKE_CURRENT_BINARY_DIR}/config/create.mysql)
configure_file(config/update.mysql.in ${CMAKE_CURRENT_BINARY_DIR}/config/update.mysql)
configure_file(config/database.json.in ${JSON_CONF_PATH})

