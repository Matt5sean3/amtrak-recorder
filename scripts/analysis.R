#!/usr/bin/Rscript

require("DBI")
require("RMySQL")

db <- dbConnect(MySQL(), 
                dbname="train_database", 
                username="trains", 
                password="train spotter",
                host="localhost",
                port=3306)


print("START ANALYSIS")
regulator = file("stdin", 'r')
# Regulate using stdin
while(readChar(regulator, 1, TRUE) == " ") {
  print("CYCLE ANALYSIS")
  # Fetch all the trains
  trains <- dbGetQuery(db, "SELECT TrainNum, OrigTime FROM trains;")
  
  # Fetch all the stations
  stations <- dbGetQuery(db, "SELECT Code FROM stations;")

  # Fetch all the stops with schedule data
  stops <- dbGetQuery(db, "SELECT StationCode, \
                                  OrigTime, \
                                  TrainNum, \
                                  ScheduledArrival, \
                                  ScheduledDeparture FROM stops;")
  
  # Fetch all the completed departures
  departures <- dbGetQuery(db, "SELECT TrainNum, OrigTime, StationCode, Time FROM departures;")
  arrivals <- dbGetQuery(db, "SELECT TrainNum, OrigTime, StationCode, Time FROM departures;")
  # Form a mapping between two station codes and a sample set
  # Expected is meant to form an initial value
  expected = list()
  
  # Build the stops into schedules
  for(i in 1:dim(trains)[1]) {
    train = trains[i, ]
    schedule = stops[which(
      unlist(stops["TrainNum"]) == unlist(train["TrainNum"]) & 
      unlist(stops["OrigTime"]) == unlist(train["OrigTime"])), ]
    # Re-order the schedule by scheduled departure
    schedule = schedule[order(unlist(schedule["ScheduledDeparture"]), 
                              na.last=TRUE), ]
    
    # Find the latest time the train is set to be at each station
    scheduled_arrivals = NULL
    # There's probably a more terse way to do this
    for(j in 1:dim(schedule)[1]) {
      stop = schedule[j,]
      arr = unlist(stop["ScheduledArrival"])
      dep = unlist(stop["ScheduledDeparture"])
      scheduled_arrivals = c(scheduled_arrivals, if(is.na(arr)) dep else arr)
    }
  }
}
close(regulator)

dbDisconnect(db)

