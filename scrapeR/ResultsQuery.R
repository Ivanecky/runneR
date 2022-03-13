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
  runner_lines = as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division", FALSE, "1"))
  # Rename columns
  names(runner_lines) = c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION", "IS_FIELD", "MARK_TIME")
  # Reformat var
  runner_lines <- runner_lines %>%
    mutate(
      YEAR = as.character(YEAR),
      EVENT = as.character(EVENT),
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
    
    source("/Users/samivanecky/git/runneR/scrapeR/Scraping_Fxns.R")
    
    # Make function call
    # runner_temp <- runnerScrape(wp[i])
    
    tryCatch({
        # Get runner
        tempRunner <- runnerScrape(runnerLinks[i])
        # Return value
        return(tempRunner)
      },  
      error=function(cond) {
        message("Here's the original error message:")
        message(cond)
        # Choose a return value in case of error
        errorLinks <- append(errorLinks, runnerLinks[i])
        # Sys.sleep(60)
        return(NA)
      }
    )
    
    # runner_temp
  }
  
  stopCluster(cl)
  
  # Return data
  return(runner_lines)
}