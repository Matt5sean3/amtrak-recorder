#!/usr/bin/Rscript

require("DBI")
require("RMySQL")

db <- dbConnect(MySQL(), 
                dbname="train_database", 
                username="trains", 
                password="train spotter",
                host="localhost",
                port=3306)

# === FETCH INFORMATION ABOUT ALREADY PROCESSED LINKS ===
tab_name = "station_link"
if(dbExistsTable(db, tab_name)) {
  oldLinkRecords <- dbReadTable(db, tab_name)
} else {
  oldLinkRecords <- NULL
}

regulator = file("stdin")

# Regulate using stdin
while(readChar(regulator, 1, TRUE) == " ") {
  linkRecords <- data.frame(
    FromStation = NULL,
    ToStation = NULL,
    Duration = NULL,
    TrainNum = NULL,
    OrigTime = NULL,
    Source = NULL
    )
  
  # I suspect some sort of caching behavior is happening with dbReadTable
  # === FETCH ALL THE TRAINS ===
  trains <- dbReadTable(db, "trains")

  # === EXTRACT ALL STOPS WHICH HAVE BEEN TRAVELED THROUGH ===
  stops <- dbReadTable(db, "stops")
  
  traveled_stops <- stops[which(
      !is.na(unlist(stops["ActualArrival"])) | 
      !is.na(unlist(stops["ActualDeparture"]))
      ), ]

  
  offset <- 0
  for(i in 1:nrow(trains)) {
    # === Trains are identified by their number and origin time ===
    trainNum <- unlist(trains[i, "TrainNum"])
    trainOrig <- unlist(trains[i, "OrigTime"])
    identity <- paste(trainNum, trainOrig)

    # === EXTRACT TRAVELED SCHEDULE FOR THIS TRAIN ===
    traveled_schedule = traveled_stops[which(
      unlist(traveled_stops["TrainNum"]) == trainNum & 
      unlist(traveled_stops["OrigTime"]) == trainOrig), ]

    # === RE-ORDER SCHEDULE CHRONOLOGICALLY BY SCHEDULED DEPARTURE ===
    traveled_schedule = traveled_schedule[order(
      unlist(traveled_schedule["ScheduledDeparture"]), 
      na.last=TRUE), ]

    rows = nrow(traveled_schedule)

    # === EXTRACT LINKS WHICH HAVE BEEN COMPUTED FOR THIS TRAIN ===
    computed_links <- oldLinkRecords[which(
      unlist(oldLinkRecords["TrainNum"]) == trainNum &
      unlist(oldLinkRecords["OrigTime"]) == trainOrig), ]
    
    # === DROP ENTRIES THAT ARE AREADY FULLY COMPUTED ===
    # TODO: This could cause links between non-adjacent stations to be computed
    # That's generally a bad thing
    rows = nrow(traveled_schedule)
    if(rows > 0 && nrow(computed_links) > 0) {
      uncomputed = rep(TRUE, nrow(traveled_schedule))
      for(row in 1:rows) {
        if(row < rows) {
          from = traveled_schedule[row, "StationCode"]
          to = traveled_schedule[row + 1, "StationCode"]
          fromComputed = any(
            computed_links["FromStation"] == from & 
            computed_links["ToStation"] == to)
        } else {
          # Last station shouldn't have a link from it
          fromComputed = TRUE
        }
        if(row > 1) {
          from = traveled_schedule[row - 1, "StationCode"]
          to = traveled_schedule[row, "StationCode"]
          toComputed = any(
            computed_links["FromStation"] == from & 
            computed_links["ToStation"] == to)
        } else {
          # First station shouldn't have a link to it
          toComputed = TRUE
        }
        uncomputed[[row]] = !(fromComputed && toComputed)
      }
      traveled_schedule = traveled_schedule[which(uncomputed), ]
    }

    # === NEED AT LEAST TWO ROWS FOR TRAVEL TIME CALCULATIONS ===
    if(nrow(traveled_schedule) > 1) {
      
      # ATTEMPT TO FILL IN MISSING DEPARTURE TIMES
      # ASSUME ON TIME DEPARTURE IF ARRIVAL IS SOON ENOUGH
      undeparted_idxs = which(is.na(unlist(traveled_schedule["ActualDeparture"])) & 
        unlist(traveled_schedule["ScheduledDeparture"]) > 
          unlist(traveled_schedule["ActualArrival"]))
      traveled_schedule[undeparted_idxs, "ActualDeparture"] = 
        traveled_schedule[undeparted_idxs, "ScheduledDeparture"]
      # ASSUME IMMEDIATE DEPARTURE FOR THE REMAINING UNDEFINED DEPARTURES
      undeparted_idxs = which(is.na(unlist(traveled_schedule["ActualDeparture"])))
      traveled_schedule[undeparted_idxs, "ActualDeparture"] = 
        traveled_schedule[undeparted_idxs, "ActualArrival"]
      
      # === CONVERT ARRIVAL AND DEPARTURE TIMES TO TIMESTAMP ===
      rows <- nrow(traveled_schedule)
      departures = unlist(traveled_schedule[
        1:(rows - 1), "ActualDeparture"])
      departure_ts = tapply(departures, 
                            1:length(departures),
                            as.POSIXct, 
                            tz = "GMT", 
                            format = "%Y-%m-%d %H:%M:%S")
      arrivals = unlist(traveled_schedule[2:rows, "ActualArrival"])
      arrival_ts = tapply(arrivals, 
                          1:length(arrivals),
                          as.POSIXct, 
                          tz = "GMT", 
                          format = "%Y-%m-%d %H:%M:%S")
      # === CALCULATE TRAVEL TIMES ===
      travel_time = arrival_ts - departure_ts
      # === NAME THE TRAVEL TIMES BY STATION TRAVELED FROM AND TO ===
      fromStations <- NULL
      toStations <- NULL
      fromStations = traveled_schedule[1:(rows - 1), "StationCode"]
      toStations = traveled_schedule[2:rows, "StationCode"]
      # === REASSOCIATE THE TRAVEL TIMES WITH THE TRAIN AND ORIGIN NUMBER ===
      linkRecords <- rbind(linkRecords,
        data.frame(
          FromStation = fromStations,
          ToStation = toStations,
          TrainNum = rep(trainNum, rows - 1),
          OrigTime = rep(trainOrig, rows - 1),
          Source = rep("Amtrak", rows - 1),
          Duration = travel_time
          ))
    }
  }
  oldLinkRecords <- rbind(oldLinkRecords, linkRecords)
  if(nrow(linkRecords) > 0) {
    # === CREATE THE LINK TABLE IF NEEDED ===
    if(!dbExistsTable(db, tab_name)) {
      # Create the table with a constraint
      tableQuery = paste("CREATE TABLE ", dbQuoteIdentifier(db, tab_name), " (
        ", dbQuoteIdentifier(db, "FromStation"), " CHAR(3),
        ", dbQuoteIdentifier(db, "ToStation"), " CHAR(3),
        ", dbQuoteIdentifier(db, "TrainNum"), " INT,
        ", dbQuoteIdentifier(db, "OrigTime"), " DATETIME,
        ", dbQuoteIdentifier(db, "Source"), " CHAR(10),
        ", dbQuoteIdentifier(db, "Duration"), " INT, 
        CONSTRAINT pk_FromToTrainOrig PRIMARY KEY(
          FromStation, ToStation, TrainNum, OrigTime, Source)
        )", sep="")
      dbSendQuery(db, tableQuery)
    }
    # === RECORD TRAVEL TIMES IN A TABLE ===
    names = paste("(", paste(dbQuoteIdentifier(db, names(linkRecords)), collapse = ", "), ")", sep="")
    # Coerce everything to characters
    entries = paste("(", 
      dbQuoteString(db, as.character(unlist(linkRecords["FromStation"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["ToStation"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["TrainNum"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["OrigTime"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["Source"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["Duration"]))),
      ")", sep="", collapse = ",\n")
    dataQuery = paste("INSERT INTO", dbQuoteIdentifier(db, tab_name), names,
        "VALUES", entries, sep = " ");
    warnings()
    dbSendQuery(db, dataQuery)
  }
  print("CALCULATIONS COMPLETED")
}
close(regulator)

warnings()

dbDisconnect(db)
