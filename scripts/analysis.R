#!/usr/bin/Rscript

require("DBI")
require("RMySQL")

# === DEFINE FUNCTIONS ===

# Include the prediction functions
source("scripts/predict.R")

# === USED TO FILTER OUT STOPS FOR WHICH CALCULATIONS HAVE BEEN PERFORMED
# Not perfect as some exceptional cases can slip through the cracks
filter_linked_stops <- function(stops, links) {
  nstops <- nrow(stops)
  nlinks <- nrow(links)
  if(nstops > 0 && nlinks > 0) {
    uncomputed = rep(TRUE, nrow(stops))
    for(row in 1:nstops) {
      # Breaks down in the case where the first row
      uncomputed[[row]] <- !((row >= nstops || any(
          links["FromStation"] == stops[row, "StationCode"] & 
          links["ToStation"] == stops[row + 1, "StationCode"])) &&
        (row <= 1 || any(
          links["FromStation"] == stops[row - 1, "StationCode"] & 
          links["ToStation"] == stops[row, "StationCode"])))
    }
    return(stops[which(uncomputed), ])
  }
  return(stops)
}

# This is slower but fills the cracks
# TODO: this is actually painfully slow and is only needed
# in some relatively easy to define cases
has_link <- function(from, to, links) {
  nlinks <- nrow(links)
  if(nlinks > 0) {
    return(any(unlist(links["FromStation"]) == from &
      unlist(links["ToStation"]) == to))
  }
  return(FALSE)
}

# The links need to be sorted and properly stripped
calculate_links <- function(stops, existing_links) {
  # === NEED AT LEAST TWO ROWS FOR TRAVEL TIME CALCULATIONS ===
  stops <- filter_linked_stops(stops, existing_links)
  nstops <- nrow(stops)
  if(nstops < 2) {
    return(NULL)
  }
  # ATTEMPT TO FILL IN MISSING DEPARTURE TIMES
  # ASSUME ON TIME DEPARTURE IF ARRIVAL IS SOON ENOUGH
  undeparted_idxs = which(is.na(unlist(stops["ActualDeparture"])) & 
    unlist(stops["ScheduledDeparture"]) > unlist(stops["ActualArrival"]))
  stops[undeparted_idxs, "ActualDeparture"] = 
    stops[undeparted_idxs, "ScheduledDeparture"]
  # ASSUME IMMEDIATE DEPARTURE FOR THE REMAINING UNDEFINED DEPARTURES
  undeparted_idxs = which(is.na(unlist(stops["ActualDeparture"])))
  stops[undeparted_idxs, "ActualDeparture"] = 
    stops[undeparted_idxs, "ActualArrival"]
  
  # === CONVERT ARRIVAL AND DEPARTURE TIMES TO TIMESTAMP ===
  rows <- nrow(stops)
  departures = unlist(stops[
    1:(nstops - 1), "ActualDeparture"])
  departure_ts = tapply(departures, 
                        1:length(departures),
                        as.POSIXct, 
                        tz = "GMT", 
                        format = "%Y-%m-%d %H:%M:%S")
  arrivals = unlist(stops[2:nstops, "ActualArrival"])
  arrival_ts = tapply(arrivals, 
                      1:length(arrivals),
                      as.POSIXct, 
                      tz = "GMT", 
                      format = "%Y-%m-%d %H:%M:%S")
  # === CALCULATE TRAVEL TIMES ===
  travel_time = arrival_ts - departure_ts
  fromStations = stops[1:(nstops - 1), "StationCode"]
  toStations = stops[2:nstops, "StationCode"]
  # === REASSOCIATE THE TRAVEL TIMES WITH THE TRAIN AND ORIGIN NUMBER ===
  ret <- data.frame(
    FromStation = fromStations,
    ToStation = toStations,
    TrainNum = rep(stops[1, "TrainNum"], nstops - 1),
    OrigTime = rep(stops[1, "OrigTime"], nstops - 1),
    Source = rep("Amtrak", nstops - 1),
    Duration = travel_time
    )
  # === DROP ALREADY CALCULATED ENTRIES THAT SLIPPED THROUGH ===
  computed = rep(FALSE, nrow(ret))
  for(i in 1:nrow(ret)) {
    computed[[i]] <- has_link(ret[i, "FromStation"], 
                              ret[i, "ToStation"], 
                              existing_links)
  }
  return(ret[which(!computed), ])
}


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
  trains <- dbGetQuery(db, "SELECT TrainNum, OrigTime FROM trains WHERE State=\"Active\";")

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

    # === EXTRACT LINKS WHICH HAVE BEEN COMPUTED FOR THIS TRAIN ===
    computed_links <- oldLinkRecords[which(
      unlist(oldLinkRecords["TrainNum"]) == trainNum &
      unlist(oldLinkRecords["OrigTime"]) == trainOrig), ]
    # === DROP ENTRIES THAT ARE AREADY FULLY COMPUTED ===
    # TODO: This could cause links between non-adjacent stations to be computed
    # That's generally a bad thing
    linkRecords <- rbind(linkRecords, calculate_links(traveled_schedule, computed_links))
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
  cat("=== GENERATE PREDICTIONS ===\r\n")
  prediction_time = Sys.time()
  for(i in 1:nrow(trains)) {
    # For now, deactivate this portion so that I can keep gathering data
    trainNum <- unlist(trains[i, "TrainNum"])
    trainOrig <- unlist(trains[i, "OrigTime"])
    cat("PREDICTING TRAIN ", trainNum, "::", trainOrig, "\r\n", sep = "")
    schedule <- retrieve_stops(db, trainNum, trainOrig)
    stations <- unlist(schedule$stations)
    link_history <- retrieve_link_history(db, unlist(stations))
    for(i in 1:length(link_history)) {
      # Ensure all links have at least one entry
      if(length(unlist(link_history[[i]])) == 0) {
        if(!is.na(schedule$scheduled_arrivals[[i]])) {
          dt <- unclass(schedule$scheduled_arrivals[[i]]) - unclass(schedule$scheduled_departures[[i]])
        } else {
          dt <- unclass(schedule$scheduled_departures[[i + 1]]) - unclass(schedule$scheduled_departures[[i]])
        }
        link_history[i] <- dt
      }
      if(is.na(unlist(link_history[[i]]))) {
        print("OVERWRITING LACKING LINK HISTORY")
        print("TRAIN")
        print(trainNum)
        print("ORIGIN TIME")
        print(trainOrig)
        print("INDEX")
        print(i)
        if(!is.na(schedule$scheduled_arrivals[[i]])) {
          dt <- unclass(schedule$scheduled_arrivals[[i]]) - unclass(schedule$scheduled_departures[[i]])
        } else {
          dt <- unclass(schedule$scheduled_departures[[i + 1]]) - unclass(schedule$scheduled_departures[[i]])
        }
        print(dt)
        link_history[[i]] <- dt
        print(link_history[[i]])
      }
    }
    scheduled_arrivals <- schedule$scheduled_arrivals
    scheduled_departures <- schedule$scheduled_departures
    actual_arrivals <- schedule$actual_arrivals
    actual_departures <- schedule$actual_departures
    # need to add arrivals
    if(length(actual_arrivals) < length(actual_departures) - 1) {
      # Account for weird arrival cases
      for(i in (length(actual_arrivals) + 1):length(actual_departures)) {
        actual_arrivals[[i]] <- scheduled_arrivals[[i]]
      }
    }
    # need to add departures
    if(length(actual_departures) < length(actual_arrivals)) {
      for(i in (length(actual_departures) + 1):length(actual_arrivals)) {
        actual_departures[[i]] <- scheduled_departures[[i]]
      }
    }
    dpar <- predictSchedule(unlist(scheduled_arrivals),
      unlist(scheduled_departures),
      unlist(actual_arrivals),
      unlist(actual_departures),
      link_history,
      1000)
    for(col in 1:ncol(dpar)) {
      link = ceiling(col / 2)
      origT = prediction_time - unclass(prediction_time)
      if(col %% 2 == 1) {
        tableName <- "departure_predictions"
        station <- stations[link]
        if((col + 1) / 2 <= length(actual_departures)) {
          # Skips departures that have already departed
          next
        }
      } else {
        tableName <- "arrival_predictions"
        station <- stations[link + 1]
        if(col / 2 <= length(actual_arrivals)) {
          # Skips arrivals that have already arrived
          next
        }
      }
      trainNumStr <- paste(trainNum)
      query <- paste("INSERT INTO ", dbQuoteIdentifier(db, tableName),
        "(TrainNum, OrigTime, Station, RecordTime, Time, Source) VALUE 
        (", dbQuoteString(db, trainNumStr), 
        ", ", dbQuoteString(db, strftime(trainOrig, tz = "GMT")), 
        ", ", dbQuoteString(db, station), 
        ", ", dbQuoteString(db, strftime(prediction_time, tz = "GMT")), 
        ", ", dbQuoteString(db, strftime(as.POSIXct(median(dpar[, col]), origin = origT, tz = "GMT"), tz = "GMT")), 
        ", ", dbQuoteString(db, "Arctan"), ")", sep = "")
      dbSendQuery(db, query)
    }
  }
  print("CALCULATIONS COMPLETED")
}
close(regulator)

warnings()

dbDisconnect(db)
