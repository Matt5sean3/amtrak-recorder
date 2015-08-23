#!/usr/bin/Rscript

require("DBI")
require("RMySQL")

# TODO: examine times waiting at a station for a late train

# === DEFINE FUNCTION ===
str2date <- function(str) {
  return(as.POSIXct(unlist(str), tz = "GMT", format = "%Y-%m-%d %H:%M:%S"))
}

# === CREATES ARRIVAL, DEPARTURE, AND TRAVEL TIME PREDICTION MATRIXES ===
predictSchedule <- function(scheduled_arrivals, 
                            scheduled_departures, 
                            actual_arrivals,
                            actual_departures,
                            link_history, 
                            nsamples) {
  # Require that there are just as many scheduled arrivals as departures
  if(length(scheduled_arrivals) != length(scheduled_departures)) {
    stop("There must be an equal number of scheduled arrivals and departures")
  }
  nlinks <- length(scheduled_arrivals)
  nkArr <- length(actual_arrivals)
  nkDep <- length(actual_departures)
  # Require that there be at most one more departure than arrival
  if(nkDep != nkArr && nkDep != nkArr + 1) {
    stop("Must have at most one more actual departures than arrivals")
  }
  # Interleave actual arrivals and departures
  adpar <- NULL
  if(nkDep > 0) {
    adpar[2 * 1:nkDep - 1] <- actual_departures
    if(nkArr > 0) {
      adpar[2 * 1:nkArr] <- actual_arrivals
    }
  }
  # Departure and Arrival times
  dpar <- matrix(NA, nsamples, nlinks * 2) 
  # Insert known values into matrix
  # adpar is null when there are no known values
  if(!is.null(adpar)) {
    # Known columns
    kCols <- length(adpar)
    dpar[, 1:kCols] <- matrix(rep(adpar, nsamples), nsamples, kCols, byrow = TRUE)
  }
  # Predict the NA columns
  for(i in 1:nlinks) {
    col <- i * 2 - 1
    if(is.na(dpar[1, col])) {
      # Predict departure
      depT <- scheduled_departures[[i]]
      isLate <- dpar[, col - 1] > depT
      lateIdxs <- which(isLate)
      onTimeIdxs <- which(!isLate)
      dpar[lateIdxs, col] <- dpar[lateIdxs, col - 1]
      dpar[onTimeIdxs, col] <- rep(depT, length(onTimeIdxs))
    }
    col <- i * 2
    if(is.na(dpar[1, col])) {
      # Predict arrival
      arrival <- unlist(dpar[, col - 1])
      # TODO: a hard to debug issue can arise when there's no data for a given link
      if(is.na(link_history[[i]])) {
        print(i)
      }
      dt_sample <- sample(unlist(link_history[[i]]), nsamples, replace = TRUE)
      dpar[, col] <- arrival + dt_sample
    }
  }
  return(dpar)
}

retrieve_stops <- function(db, trainNum, origTime) {
  query <- paste("SELECT 
      StationCode, 
      ScheduledArrival, 
      ScheduledDeparture, 
      ActualArrival, 
      ActualDeparture 
    FROM stops WHERE
      TrainNum=", dbQuoteString(db, paste(trainNum)), " AND 
      OrigTime=", dbQuoteString(db, origTime), " ORDER BY 
      ISNULL(ScheduledDeparture), ScheduledDeparture;", 
    sep="")
  stops <- dbGetQuery(db, query)

  nlinks <- nrow(stops) - 1
  scheduled_departures <- 
    as.POSIXct(stops[1:nlinks, "ScheduledDeparture"], 
    tz = "GMT", format = "%Y-%m-%d %H:%M:%S")
  
  scheduled_arrivals <- 
    as.POSIXct(stops[2:(nlinks + 1), "ScheduledArrival"], 
    tz = "GMT", format = "%Y-%m-%d %H:%M:%S")
  
  
  # TODO, can sometimes be a too large array
  actual_departures <- 
    as.POSIXct(stops[1:nlinks, "ActualDeparture"], 
    tz = "GMT", format = "%Y-%m-%d %H:%M:%S")

  if(any(!is.na(actual_departures))) {

    actual_departures <- actual_departures[
      1:max(which(!is.na(unlist(actual_departures))))]
  } else {
    actual_departures <- list()
  }

  actual_arrivals <- 
    as.POSIXct(stops[2:(nlinks + 1), "ActualArrival"], 
    tz = "GMT", format = "%Y-%m-%d %H:%M:%S")

  if(any(!is.na(actual_arrivals))) {
  
    actual_arrivals <- actual_arrivals[
      1:max(which(!is.na(unlist(actual_arrivals))))]
  } else {
    actual_arrivals <- list()
  }

  return(list(
    stations = stops["StationCode"],
    scheduled_departures = scheduled_departures,
    scheduled_arrivals = scheduled_arrivals,
    actual_departures = actual_departures,
    actual_arrivals = actual_arrivals
    ))
}

retrieve_link_history <- function(db, stops) {
  link_history <- list()
  for(i in 1:(length(stops) - 1)) {
    link_history[[i]] <- dbGetQuery(db, paste(
      "SELECT Duration FROM station_link WHERE FromStation=", 
      dbQuoteString(db, stops[[i]]), " AND ToStation=", 
      dbQuoteString(db, stops[[i + 1]]), ";", 
      sep = ""))
  }
  return(link_history)
}

