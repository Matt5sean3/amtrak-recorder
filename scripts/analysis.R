#!/usr/bin/Rscript

require("DBI")
require("RMySQL")

db <- dbConnect(MySQL(), 
                dbname="train_database", 
                username="trains", 
                password="train spotter",
                host="localhost",
                port=3306)


regulator = file("stdin", 'r')
# Regulate using stdin
while(readChar(regulator, 1, TRUE) == " ") {
  linkRecords <- data.frame(
    Link = NULL,
    Duration = NULL,
    TrainNum = NULL,
    OrigTime = NULL,
    Source = NULL
    )
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
    trainNum = unlist(trains[i, "TrainNum"])
    trainOrig = unlist(trains[i, "OrigTime"])

    # === EXTRACT SCHEDULE FOR THIS TRAIN ===
    schedule = stops[which(
      unlist(stops["TrainNum"]) == trainNum & 
      unlist(stops["OrigTime"]) == trainOrig), ]

    # === RE-ORDER SCHEDULE CHRONOLOGICALLY BY SCHEDULED DEPARTURE ===
    schedule = schedule[order(unlist(schedule["ScheduledDeparture"]), 
                              na.last=TRUE), ]

    # === CONVERT DEPARTURE TIMES TO TIMESTAMP ===
    #scheduled_departures = unlist(schedule[1:(dim(schedule)[[1]] - 1), "ScheduledDeparture"])
    #departure_ts = tapply(scheduled_departures, 
    #                      1:length(scheduled_departures), 
    #                      as.POSIXct, 
    #                      tz = "GMT", 
    #                      format = "%Y-%m-%d %H:%M:%S")

    # === CONVERT ARRIVAL TIMES TO TIMESTAMP ===
    #arrival_ts = NULL
    #for(j in 2:dim(schedule)[1]) {
    #  stop = schedule[j,]
    #  arr = unlist(stop["ScheduledArrival"])
    #  dep = unlist(stop["ScheduledDeparture"])
    #  arrival_ts = c(arrival_ts, 
    #                 as.POSIXct(if(is.na(arr)) dep else arr, 
    #                            tz = "GMT", 
    #                            format = "%Y-%m-%d %H:%M:%S"))
    #}

    # === CALCULATE SCHEDULED TIME TO TRAVEL BETWEEN STATIONS ===
    #for(j in 1:length(arrival_ts)) {
    #  link = paste(schedule[j, "StationCode"], 
    #               schedule[j + 1, "StationCode"], sep="_")
    #  dt = arrival_ts[j] - departure_ts[j]
    #  if(!(link %in% names(expected))) {
    #    expected[link] = dt
    #  }
    #}

    # === USE STATION CODES AS ROW NAMES ===
    stop_set = list()
    row.names(schedule) <- unlist(schedule["StationCode"])

    # === EXTRACT THE PORTION OF THE SCHEDULE THAT HAS HAPPENED ===
    traveled_schedule = schedule[which(
      !is.na(unlist(schedule["ActualArrival"])) | 
      !is.na(unlist(schedule["ActualDeparture"]))
      ), ]
    # ATTEMPT TO FILL IN MISSING DEPARTURE TIMES
    # ASSUME ON TIME DEPARTURE
    undeparted_idxs = which(is.na(unlist(traveled_schedule["ActualDeparture"])) & 
      unlist(traveled_schedule["ScheduledDeparture"]) > 
        unlist(traveled_schedule["ActualArrival"]))
    traveled_schedule[undeparted_idxs, "ActualDeparture"] = 
      traveled_schedule[undeparted_idxs, "ScheduledDeparture"]
    # ASSUME IMMEDIATE DEPARTURE FOR THE REMAINING UNDEFINED DEPARTURES
    undeparted_idxs = which(is.na(unlist(traveled_schedule["ActualDeparture"])))
    traveled_schedule[undeparted_idxs, "ActualDeparture"] = 
      traveled_schedule[undeparted_idxs, "ActualArrival"]

    rows = nrow(traveled_schedule)
    # === NEED AT LEAST TWO ROWS FOR TRAVEL TIME CALCULATIONS ===
    if(rows > 1) {
      # === CONVERT ARRIVAL AND DEPARTURE TIMES TO TIMESTAMP ===
      actual_departures = unlist(traveled_schedule[
        1:(rows - 1), "ActualDeparture"])
      if(length(actual_departures > 0)) {
        actual_departure_ts = tapply(actual_departures, 
                                     1:length(actual_departures),
                                     as.POSIXct, 
                                     tz = "GMT", 
                                     format = "%Y-%m-%d %H:%M:%S")
      } else {
        actual_departure_ts = NULL
      }
      actual_arrivals = unlist(traveled_schedule[2:rows, "ActualArrival"])
      if(length(actual_arrivals) > 0) {
        actual_arrival_ts = tapply(actual_arrivals, 
                                   1:length(actual_arrivals),
                                   as.POSIXct, 
                                   tz = "GMT", 
                                   format = "%Y-%m-%d %H:%M:%S")
      } else {
        actual_arrival_ts = NULL
      }
      # === CALCULATE TRAVEL TIMES ===
      if(rows > 1) {
        actual_travel_time = actual_arrival_ts - actual_departure_ts
      }
      # === NAME THE TRAVEL TIMES BY STATION TRAVELED FROM AND TO ===
      rnames <- NULL
      for(j in 1:(rows - 1)) {
        rnames = c(rnames, paste(traveled_schedule[j, "StationCode"], 
                                 traveled_schedule[j, "StationCode"], sep = "_"))
      }
      # === REASSOCIATE THE TRAVEL TIMES WITH THE TRAIN AND ORIGIN NUMBER ===
      # === RECORD TRAVEL TIMES IN A TABLE ===
      linkRecord = data.frame(
        Link = rnames,
        Duration = actual_travel_time,
        TrainNum = rep(trainNum, rows - 1),
        OrigTime = rep(trainOrig, rows - 1),
        Source = rep("Amtrak", rows - 1)
        )
      linkRecords <- rbind(linkRecords, linkRecord)
    }
  }
  print(linkRecords)
  # 
  print("CALCULATIONS COMPLETED")
}
close(regulator)

warnings()

dbDisconnect(db)
