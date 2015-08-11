#!/usr/bin/Rscript

# Needs GDAL to handle projection
require("rgdal")
# Needs spatial data package for containing data
require("sp")
# Needs GEOS package for calculating distance
require("rgeos")

# DBI via RMySQL retrieves the data
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
  # Ideally has many readings for routes between stations
  # Without sufficient shape data, assume a straight line between stations
  
  # Still needs the schedule to know the stations
  
}

