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

sourceName <- "ArctanGPS"

trains <- dbGetQuery(db, "SELECT TrainNum, OrigTime, State FROM trains;")
computedTable <- dbGetQuery(db, paste0("SELECT DISTINCT TrainNum, OrigTime FROM arrival_predictions WHERE Source=",
  dbQuoteString(db, sourceName), ";"))
computedTrains <- paste0(computedTable["TrainNum"], computedTable["OrigTime"])

# Retrieve stops information
stopLocations <- dbGetQuery(db, "SELECT DISTINCT Code, Latitude, Longitude FROM stations ORDER BY DateModif DESC;")

# Use the Code as row names
row.names(stopLocations) <- unlist(stopLocations["Code"])

for(i in 1:nrow(trains)) {
  # Process a train
  trainNum <- unlist(trains[i, "TrainNum"])
  origTime <- unlist(trains[i, "OrigTime"])
  state <- unlist(trains[i, "State"])
  if(state == "Completed") {
    # Skip already processed groups of completed trains
    next
  }
  # Retrieve the readings
  readings <- dbGetQuery(db, paste0(
    "SELECT Latitude, Longitude, Time, Speed FROM readings WHERE TrainNum=",
    dbQuoteString(db, paste0(trainNum)), " AND OrigTime=", 
    dbQuoteString(db, origTime), " ORDER BY Time;"))
  if(nrow(readings) == 0) {
    # Skip trains that are no longer active
    next
  }
  # Filter out na lats and lons

  # Retrieve each stop
  schedule <- dbGetQuery(db, paste0("SELECT StationCode FROM stops WHERE TrainNum=",
    dbQuoteString(db, paste0(trainNum)), " AND OrigTime=", dbQuoteString(db, origTime), ";"))
  
  stationLocations = stopLocations[unlist(schedule["StationCode"]), ]

  # For now, just plot the unprojected points for some basic visualization
  jpeg(paste0(trainNum, "-", origTime, ".jpg"))
  lons <- unlist(readings["Longitude"])
  lats <- unlist(readings["Latitude"])
  valids <- which(!(is.na(lons) | is.na(lats)))
  lons <- lons[valids]
  lats <- lats[valids]
  stationLons <- unlist(stationLocations["Longitude"])
  stationLats <- unlist(stationLocations["Latitude"])
  valids <- !(is.na(stationLons) | is.na(stationLats))
  stationLons <- stationLons[valids]
  stationLats <- stationLats[valids]
  xlims <- c(min(lons, stationLons), max(lons, stationLons))
  ylims <- c(min(lats, stationLats), max(lats, stationLats))
  
  plot(x = lons, 
    y = lats, 
    xlab = "Latitude",
    ylab = "Longitude",
    type = "l", 
    xlim = xlims,
    ylim = ylims,
    main = paste0("Train ", trainNum, " Starting ", origTime))

  points(stationLons,
    stationLats, 
    col = 'blue')
  
  dev.off()
  # Determine the point closest to the stop for the path as well as sitting time
}


# Derive departure and arrival times at stations from GPS readings

# Needs GPS readings for each station
# Expect WGS 84/EPSG:4326 coordinates
# Ideally has many readings for routes between stations
# Without sufficient shape data, assume a straight line between stations

# Still needs the schedule to know the stations
# First step is associating the reading with a segment
  

