#!/usr/bin/Rscript

require("DBI")
require("RMySQL")

db <- dbConnect(MySQL(), 
  dbname = "train_database",
  username = "trains",
  password = "train spotter",
  host = "localhost",
  port = 3306)

trains <- dbGetQuery(db, "SELECT TrainNum, OrigTime FROM trains WHERE State=\"Completed\";")

for (i in 1:nrow(trains)) {
  train <- trains[i, ]
  # Needs conversion from string to POSIXct
  groundTruth <- dbGetQuery(db, paste0(
    "SELECT StationCode, ActualArrival, ScheduledArrival, ScheduledDeparture FROM stops WHERE TrainNum=",
    dbQuoteString(db, as.character(unlist(train["TrainNum"]))), " AND OrigTime=", 
    dbQuoteString(db, unlist(train["OrigTime"])), ";"))
  amtrakPredictions <- dbGetQuery(db, paste0(
    "SELECT Station, RecordTime, Time FROM arrival_predictions WHERE TrainNum=",
    dbQuoteString(db, as.character(unlist(train["TrainNum"]))), " AND OrigTime=", 
    dbQuoteString(db, unlist(train["OrigTime"])), " AND Source=",
    dbQuoteString(db, "Amtrak"), ";"))
  arctanPredictions <- dbGetQuery(db, paste0(
    "SELECT Station, RecordTime, Time FROM arrival_predictions WHERE TrainNum=",
    dbQuoteString(db, as.character(unlist(train["TrainNum"]))), " AND OrigTime=", 
    dbQuoteString(db, unlist(train["OrigTime"])), " AND Source=",
    dbQuoteString(db, "Arctan"), ";"))
  # Plot the accuracy of the estimates over time for each station
  for(j in 1:nrow(groundTruth)) {
    if(nrow(amtrakPredictions) == 0) {
      next
    }
    stop <- groundTruth[j, ]
    amtrakSet <- amtrakPredictions[which(amtrakPredictions["Station"] == unlist(stop["StationCode"])), ]
    arctanSet <- arctanPredictions[which(arctanPredictions["Station"] == unlist(stop["StationCode"])), ]
    if(nrow(amtrakSet) == 0 || nrow(arctanSet) == 0) {
      next
    }
    jpeg(paste0(
      "prediction_", 
      unlist(train["TrainNum"]), "_", 
      unlist(train["OrigTime"]), "_", 
      unlist(stop["StationCode"]), ".jpg"))
    # Convert the values to POSIXct
    amtrakPosixRecord <- as.POSIXct(unlist(amtrakSet["RecordTime"]), format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
    amtrakPosixPredict <- as.POSIXct(unlist(amtrakSet["Time"]), format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
    arctanPosixRecord <- as.POSIXct(unlist(arctanSet["RecordTime"]), format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
    arctanPosixPredict <- as.POSIXct(unlist(arctanSet["Time"]), format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
    # Get actual arrival time
    actualArrivalTime <- as.POSIXct(unlist(stop["ActualArrival"]), format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
    # Get scheduled arrival time
    if(is.na(unlist(stop["ActualArrival"]))) {
      arrText <- unlist(stop["ScheduledDeparture"])
    } else {
      arrText <- unlist(stop["ScheduledArrival"])
    }
    # Get Actual Arrival Time
    scheduledArrivalTime <- as.POSIXct(arrText, format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
    ylim <- c(
      min(amtrakPosixPredict, arctanPosixPredict),
      max(amtrakPosixPredict, arctanPosixPredict))
    if(!is.na(actualArrivalTime)) {
      ylim <- c(min(ylim[1], actualArrivalTime), max(ylim[2], actualArrivalTime))
    }
    if(!is.na(scheduledArrivalTime)) {
      ylim <- c(min(ylim[1], actualArrivalTime), max(ylim[2], actualArrivalTime))
    }
    xlim <- c(
      min(amtrakPosixRecord, arctanPosixRecord),
      max(amtrakPosixRecord, arctanPosixRecord))
    # Create the plot
    plot(amtrakPosixRecord, amtrakPosixPredict, 
      main = "Arrival Time Prediction across Time", 
      xlab = "Time of Prediction", 
      ylab = "Predicted Arrival Time",
      col = "red",
      type = "s",
      xlim = xlim,
      ylim = ylim)
    lines(arctanPosixRecord,
      arctanPosixPredict,
      col = "blue", 
      type = "s")
    # Draw a line for actual arrival time
    lines(xlim, c(actualArrivalTime, actualArrivalTime), col = "green")
    lines(xlim, c(scheduledArrivalTime, scheduledArrivalTime), col = "gray")
    dev.off()
  }
}

dbDisconnect(db)

