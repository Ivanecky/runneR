library(tidymodels)
library(httr)
library(dplyr)
library(jsonlite)
library(RPostgreSQL)
library(DBI)
library(RSQLite)
library(reshape2)
library(stringr)
library(yaml)
library(rvest)
library(foreach)
library(doParallel)

resultsQuery <- function(wp){
  # Create a temporary dataframe for runner line item performance
  runner_lines = as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division"))
  # Rename columns
  names(runner_lines) = c("YEAR", "EVENT", "TIME", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION")
  # Reformat var
  runner_lines <- runner_lines %>%
    mutate(
      YEAR = as.character(YEAR),
      EVENT = as.character(EVENT),
      TIME = as.numeric(TIME),
      PLACE = as.numeric(PLACE),
      NAME = as.character(NAME),
      GENDER = as.character(GENDER),
      TEAM = as.character(TEAM)
    )
  
  # Detect cores
  cores <- detectCores()
  cl <- makeCluster(cores[1] - 1)
  registerDoParallel(cl)
  
  runner_lines <- foreach(i=1:length(wp), .combine = rbind) %dopar% {
    
    source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/Scraping_Fxns.R")
    
    # Make function call
    runner_temp <- runnerScrape(wp[i])
    
    runner_temp
  }
  
  stopCluster(cl)
  
  # Return data
  return(runner_lines)
}