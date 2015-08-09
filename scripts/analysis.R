#!/usr/bin/Rscript

require("DBI")
require("RMySQL")
#require("rgeos")

db <- dbConnect(MySQL(), 
                dbname="train_database", 
                username="trains", 
                password="train spotter",
                host="localhost",
                port=3306)


regulator = file("stdin", 'r')
# Regulate using stdin
while(readChar(regulator, 1, TRUE) == " ") {
  # Fetch all the trains
  trains <- dbGetQuery(db, "SELECT TrainNum, OrigTime FROM trains;")
  
  # Fetch all the stations
  stations <- dbGetQuery(db, "SELECT Code FROM stations;")

  # Fetch all the stops with schedule data
  stops <- dbGetQuery(db, "SELECT StationCode, \
                                  OrigTime, \
                                  TrainNum, \
                                  ScheduledArrival, \
                                  ScheduledDeparture, \
                                  ActualArrival, \
                                  ActualDeparture FROM stops;")

  readings <- dbGetQuery(db, "SELECT TrainNum, \
                                     Latitude, \
                                     Longitude, \
                                     Time, \
                                     State FROM readings;")
  #print("READINGS")
  #print(readings)
  
  # Form a mapping between two station codes and a sample set
  # Expected is meant to form an initial value
  expected = list()
  
  # actual contains a distribution formed of all runs
  actual = list()
  
  # Build the stops into schedules
  for(i in 1:dim(trains)[1]) {
    train = trains[i, ]

    # === EXTRACT SCHEDULE FOR THIS TRAIN ===
    schedule = stops[which(
      unlist(stops["TrainNum"]) == unlist(train["TrainNum"]) & 
      unlist(stops["OrigTime"]) == unlist(train["OrigTime"])), ]
    # === RE-ORDER SCHEDULE CHRONOLOGICALLY ===
    schedule = schedule[order(unlist(schedule["ScheduledDeparture"]), 
                              na.last=TRUE), ]

    # === CONVERT DEPARTURE TIMES TO TIMESTAMP ===
    scheduled_departures = unlist(schedule[1:(dim(schedule)[[1]] - 1), "ScheduledDeparture"])
    departure_ts = tapply(scheduled_departures, 
                          1:length(scheduled_departures), 
                          as.POSIXct, 
                          tz = "GMT", 
                          format = "%Y-%m-%d %H:%M:%S")

    # === CONVERT ARRIVAL TIMES TO TIMESTAMP ===
    arrival_ts = NULL
    for(j in 2:dim(schedule)[1]) {
      stop = schedule[j,]
      arr = unlist(stop["ScheduledArrival"])
      dep = unlist(stop["ScheduledDeparture"])
      arrival_ts = c(arrival_ts, 
                     as.POSIXct(if(is.na(arr)) dep else arr, 
                                tz = "GMT", 
                                format = "%Y-%m-%d %H:%M:%S"))
    }

    # === CALCULATE SCHEDULED TIME TO TRAVEL BETWEEN STATIONS ===
    for(j in 1:length(arrival_ts)) {
      link = paste(schedule[j, "StationCode"], 
                   schedule[j + 1, "StationCode"], sep="_")
      dt = arrival_ts[j] - departure_ts[j]
      if(!(link %in% names(expected))) {
        expected[link] = dt
      }
    }

    # === USE STATION CODES AS ROW NAMES ===
    stop_set = list()
    row.names(schedule) <- unlist(schedule["StationCode"])

    # === CONVERT ARRIVAL AND DEPARTURE TIMES TO TIMESTAMP ===
    actual_departures = unlist(schedule[
      which(!is.na(unlist(schedule["ActualDeparture"]))), 
      "ActualDeparture"])
    if(length(actual_departures > 0)) {
      actual_departure_ts = tapply(actual_departures, 
                                   1:length(actual_departures),
                                   as.POSIXct, 
                                   tz = "GMT", 
                                   format = "%Y-%m-%d %H:%M:%S")
    } else {
      actual_departure_ts = NULL
    }
    actual_arrivals = unlist(schedule[
      which(!is.na(unlist(schedule["ActualArrival"]))),
      "ActualArrival"])
    if(length(actual_arrivals) > 0) {
      actual_arrival_ts = tapply(actual_arrivals, 
                                 1:length(actual_arrivals),
                                 as.POSIXct, 
                                 tz = "GMT", 
                                 format = "%Y-%m-%d %H:%M:%S")
    } else {
      actual_arrival_ts = NULL
    }
    # === EXTRACT RELEVANT READINGS ===
    #print("SCHEDULE")
    #print(schedule)
    #print("ARRIVALS")
    #print(actual_arrival_ts)
    #print("DEPARTURES")
    #print(actual_departure_ts)

    # === CALCULATE ACTUAL TIME TO TRAVEL BETWEEN STATIONS ===
    #if(lengths_traveled > 0) {
    #  actual_travel_time = (actual_arrival_ts - actual_departure_ts[1:lengths_traveled]) / 60
    #  print(actual_travel_time)
    #  # PRINT IF THE travel time is somehow negative
    #  if(min(actual_travel_time) < 0) {
    #    print("Departures")
    #    print(actual_departures)
    #    print("Arrivals")
    #    print(actual_arrivals)
    #    print("TRAVEL TIMES")
    #    print(actual_travel_time)
    #  }
    #}
    # ===  ===
    #print("Actual Departure")
    #print(actual_departures)
    #print("Actual Arrival")
    #print(actual_arrivals)
    # For amusement, calculate the disparity between 
    # scheduled and actual arrival
  }
  #print(expected)
  print("CALCULATIONS COMPLETED")
}
close(regulator)

warnings()

dbDisconnect(db)
