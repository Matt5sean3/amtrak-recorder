#!/usr/bin/Rscript

require("rgeos")
require("DBI")
require("RMySQL")

db <- dbConnect(MySQL(), 
  dbname="train_database",
  username="trains",
  password="train spotter",
  host="localhost",
  port=3306)

regulator = file("stdin", "r")

# Derive departure and arrival times at stations from GPS readings

while(readChar(regulator, 1, TRUE) == " ") {
  # Needs GPS readings for each station
  # Expect WGS 84/EPSG:4326 coordinates
  # Ideally has detailed shape files for routes between stations
  
  # Still needs the schedule to know the stations
  
}

