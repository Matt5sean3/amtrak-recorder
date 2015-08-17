#!/usr/bin/Rscript

require("DBI")
require("RMySQL")

# === DEFINE FUNCTION ===
str2date <- function(str) {
  return(as.POSIXct(unlist(str), tz = "GMT", format = "%Y-%m-%d %H:%M:%S"))
}

# === Open the database connection ===
db <- dbConnect(MySQL(), 
  dbname = "train_database",
  username = "trains",
  password = "train spotter",
  host = "localhost",
  port = 3306)

# === READ REQUEST FROM SOCKET ===
sock <- file("stdin")

# First line is train number
# Second line is origin time
# Third line is station
lines <- readLines(sock, n = 3, ok = TRUE, warn = FALSE)
trainNum <- lines[1]
origTime <- lines[2]
station <- lines[3]

# === READ SCHEDULE TABLE ===
# Orders by ScheduleDeparture with NULLs forced to the bottom
query <- paste("SELECT 
    StationCode, 
    ScheduledArrival, 
    ScheduledDeparture, 
    ActualArrival, 
    ActualDeparture 
  FROM stops WHERE
    TrainNum=", dbQuoteString(db, trainNum), " AND 
    OrigTime=", dbQuoteString(db, origTime), " ORDER BY 
    ISNULL(ScheduledDeparture), ScheduledDeparture;", 
  sep="")
stops <- dbGetQuery(db, query)

if(nrow(stops) == 0) {
  cat("Records for that train originating at that time do not exist\r\n")
  quit("no")
}

# === GRAB THE RECORD FOR THE STATION IN QUESTION ===
stationIdx = which(unlist(stops["StationCode"]) == station)
if(length(stationIdx) == 0) {
  cat("That station is not present on the route of that train\r\n")
  quit("no")
}

# === CHECK IF THE TRAIN HAS ALREADY DEPARTED ===
if(!is.na(stops[stationIdx, "ActualDeparture"])) {
  cat(paste("The train has already departed", 
    stops[stationIdx, "ActualDeparture"], "", sep="\r\n"))
  quit("no")
}

# === START ESTIMATING DEPARTURE TIME ===
# First need to find where the train's last stop was

# Arrival is fairly consistently captured
lastArrIdx <- max(which(!is.na(unlist(stops["ActualArrival"]))))

# Departure isn't always captured, but this should do okay
# for the most part
lastDepIdx <- max(which(!is.na(unlist(stops["ActualDeparture"]))))

# Need the current time for interpreting results
t <- Sys.time()

# TODO: certain stations just don't provide a departure time
# In order to account for those stations, the readings table needs to be used
if(lastArrIdx == lastDepIdx) {
  # Departed the last station
  depT <- str2date(stops[lastDepIdx, "ActualDeparture"]) 
  cat("Train in transit from ", stops[lastDepIdx, "StationCode"], " to ", 
    stops[lastDepIdx + 1, "StationCode"], "\r\n")
} else if(stops[lastArrIdx, "ActualArrival"] < stops[lastArrIdx, "ScheduledDeparture"]) {
  # Arrived at the station on time
  depT <- str2date(stops[lastArrIdx, "ScheduledDeparture"])
  cat("Train is waiting to depart at ", stops[lastArrIdx, "StationCode"], "\r\n")
  cat("Departure expected: ", strftime(str2date(stops[lastArrIdx, "ScheduledDeparture"])), "\r\n")
} else {
  # Arrived at the station late
  # Assumes the train will leave ASAP
  cat("Train is late waiting at ", stops[lastArrIdx, "StationCode"], "\r\n")
  cat("Departure expected: ", strftime(str2date(stops[lastArrIdx, "ScheduledDeparture"])), "\r\n")
  # TODO: current time in UTC
  depT <- t
}
cat("Departure time is ", strftime(depT), "\r\n")
cat("Current time is ", strftime(t), "\r\n")
cat("Time difference is ", difftime(t, depT), "\r\n")

# === READ HISTORY FOR EACH LINK BETWEEN THE CURRENT LOCATION AND DESTINATION ===
for(i in lastArrIdx:(stationIdx - 1)) {
  from = stops[i, "StationCode"]
  to = stops[i + 1, "StationCode"]
  times <- dbGetQuery(db, paste(
    "SELECT Duration FROM station_link WHERE FromStation=", 
    dbQuoteString(db, from), " AND ToStation=", dbQuoteString(db, to), ";", 
    sep = ""))
  dt <- mean(unlist(times))
  cat("Average time to traverse ", from, " to ", to, " is ", dt / 60, " minutes\r\n")
}

# === GIVE THE SCHEDULED DEPARTURE TIME ===
if(!is.na(stops[stationIdx, "ScheduledDeparture"])) {
  cat(paste("The train is scheduled to depart", 
    stops[stationIdx, "ScheduledDeparture"], "", sep="\r\n"))
  quit("no")
}

warnings()

dbDisconnect(db)
