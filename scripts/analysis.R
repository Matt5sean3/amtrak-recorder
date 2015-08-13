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
linksProcessed <- list()

if(dbExistsTable(db, tab_name)) {
  link_data <- dbReadTable(db, tab_name)
  while(nrow(link_data) > 0) {
    trainNum = link_data[1, "TrainNum"]
    trainOrig = link_data[1, "OrigTime"]
    identity = paste(trainNum, trainOrig)
    processedSlots = unlist(link_data["TrainNum"]) == trainNum &
      unlist(link_data["OrigTime"]) == trainOrig
    # Keep the slots not in the group
    link_data = link_data[which(!processedSlots), ]
    # Count the number of links in the group
    linksProcessed[identity] = length(which(processedSlots))
  }
}

regulator <- file("stdin", 'r')


# Regulate using stdin
while(readChar(regulator, 1, TRUE) == " ") {
  linkRecords <- data.frame(
    Link = NULL,
    Duration = NULL,
    TrainNum = NULL,
    OrigTime = NULL,
    Source = NULL
    )
  
  # I suspect some sort of caching behavior is happening with dbReadTable
  # === FETCH ALL THE TRAINS ===
  #trains <- dbReadTable(db, "trains")
  trains <- dbGetQuery(db, "SELECT * FROM trains;")
  print("NUM TRAINS")
  print(nrow(trains))

  # === EXTRACT ALL STOPS WHICH HAVE BEEN TRAVELED THROUGH ===
  stops <- dbReadTable(db, "stops")
  print("NUM STOPS")
  print(nrow(stops))
  
  traveled_stops <- stops[which(
      !is.na(unlist(stops["ActualArrival"])) | 
      !is.na(unlist(stops["ActualDeparture"]))
      ), ]
  
  print("NUM TRAVELED STOPS")
  print(nrow(traveled_stops))

  offset <- 0
  for(i in 1:nrow(trains)) {
    # === Trains are identified by their number and origin time ===
    trainNum = unlist(trains[i, "TrainNum"])
    trainOrig = unlist(trains[i, "OrigTime"])
    identity <- paste(trainNum, trainOrig)
    
    # === EXTRACT TRAVELED SCHEDULE FOR THIS TRAIN ===
    traveled_schedule = traveled_stops[which(
      unlist(traveled_stops["TrainNum"]) == trainNum & 
      unlist(traveled_stops["OrigTime"]) == trainOrig), ]

    # === RE-ORDER SCHEDULE CHRONOLOGICALLY BY SCHEDULED DEPARTURE ===
    traveled_schedule = traveled_schedule[order(
      unlist(traveled_schedule["ScheduledDeparture"]), 
      na.last=TRUE), ]

    if(!(identity %in% names(linksProcessed))) {
      linksProcessed[identity] = 0
    }
    nProcessed <- linksProcessed[[identity]]
    rows = nrow(traveled_schedule)
    
    # === IGNORE ALL THE ROWS YOU'RE DONE PROCESSING ===
    if(rows > 0) {
      if(rows - 1 != linksProcessed[identity]) {
        #print(traveled_schedule)
      }
      linksProcessed[identity] <- rows - 1
      traveled_schedule = traveled_schedule[(nProcessed + 1):rows, ]
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
      rnames <- NULL
      for(j in 1:(rows - 1)) {
        rnames = c(rnames, paste(traveled_schedule[j, "StationCode"], 
                                 traveled_schedule[j + 1, "StationCode"], sep = "_"))
      }
      # === REASSOCIATE THE TRAVEL TIMES WITH THE TRAIN AND ORIGIN NUMBER ===
      linkRecords <- rbind(linkRecords,
        data.frame(
          Link = rnames,
          TrainNum = rep(trainNum, rows - 1),
          OrigTime = rep(trainOrig, rows - 1),
          Source = rep("Amtrak", rows - 1),
          Duration = travel_time
          ))
    }
  }
  
  if(nrow(linkRecords) > 0) {
    # === CREATE THE LINK TABLE IF NEEDED ===
    if(!dbExistsTable(db, tab_name)) {
      # Create the table with a constraint
      tableQuery = paste("CREATE TABLE ", dbQuoteIdentifier(db, tab_name), " (
        ", dbQuoteIdentifier(db, "Link"), " CHAR(7),
        ", dbQuoteIdentifier(db, "TrainNum"), " INT,
        ", dbQuoteIdentifier(db, "OrigTime"), " DATETIME,
        ", dbQuoteIdentifier(db, "Source"), " CHAR(10),
        ", dbQuoteIdentifier(db, "Duration"), " INT, 
        CONSTRAINT pk_LinkTrainOrig PRIMARY KEY(Link, TrainNum, OrigTime, Source)
        )", sep="")
      dbSendQuery(db, tableQuery)
    }
    # === RECORD TRAVEL TIMES IN A TABLE ===
    names = paste("(", paste(dbQuoteIdentifier(db, names(linkRecords)), collapse = ", "), ")", sep="")
    # Coerce everything to characters
    entries = paste("(", 
      dbQuoteString(db, as.character(unlist(linkRecords["Link"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["TrainNum"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["OrigTime"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["Source"]))), ", ",
      dbQuoteString(db, as.character(unlist(linkRecords["Duration"]))),
      ")", sep="", collapse = ",\n")
    # The implementation is incomplete, so write table can't be used with append
    dataQuery = paste("INSERT INTO", dbQuoteIdentifier(db, tab_name), names,
        "VALUES", entries, sep = " ");
    #cat(dataQuery)
    dbSendQuery(db, dataQuery)
  }
  print("CALCULATIONS COMPLETED")
}
close(regulator)

warnings()

dbDisconnect(db)
