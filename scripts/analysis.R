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
  
  # Fetch all the stops
  stops <- dbGetQuery(db, "SELECT StationCode, \
                                  OrigTime, \
                                  TrainNum, \
                                  ScheduledArrival, \
                                  ScheduledDeparture FROM stops;")
  # Build the stops into schedules
  for(i in 1:dim(trains)[1]) {
    train = trains[i, ]
    stops[which(data.matrix(stops["TrainNum"]) == train["TrainNum"]), ]
  }
}
close(regulator)

dbDisconnect(db)

