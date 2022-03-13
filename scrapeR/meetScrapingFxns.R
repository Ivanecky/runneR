# Code to generate line item performances from TFRRS.
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

# Load functions for scraping
source("/Users/samivanecky/git/runneR/scrapeR/Scraping_Fxns.R")
# source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/ResultsQuery.R")

# Define function
getRunnerURLs <- function(url) {
  # Get runner links
  runners <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tbody/tr/td/div/a") %>%
    html_attr("href")
  
  # Manipulate strings
  for ( i  in 1:length(runners) )
  {
    temp = runners[i]
    #temp = substring(temp, 3)
    temp = paste0("https:", temp)
    temp = substr(temp, 1, nchar(temp)-3)
    runners[i] = temp
  }
  
  # Grab only strings for athletes
  runners <- runners[grepl("athletes", runners)]
  
  # Return
  return(runners)
}

getMeetLinks <- function(url = "https://www.tfrrs.org/results_search.html") {
  # Get runner links
  meets <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tr/td/a") %>%
    html_attr("href")
  
  # Manipulate strings
  for ( i  in 1:length(meets) )
  {
    temp <- meets[i]
    temp <- paste0("https:", temp)
    temp <- gsub("[[:space:]]", "", temp)
    # temp <- paste0(substr(temp, 1, nchar(temp)-3), "tml")
    meets[i] <- temp
  }
  
  # Return
  return(meets)
}

getPLMeetLinks <- function(url) {
  
  if(class(try(url %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html())) == 'try-error') {
    print(paste0("Failed to get data for : ", url))
    return(NA)
  }
  
  # Get runner links
  meets <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tr/td/a") %>%
    html_attr("href")
  
  # Filter to results only
  meets <- meets[grepl("results", meets)]
  
  # Keep unique
  meets <- funique(meets)
  
  # Manipulate strings
  for ( i  in 1:length(meets) )
  {
    temp <- meets[i]
    temp <- paste0("https:", temp)
    temp <- gsub("[[:space:]]", "", temp)
    # temp <- paste0(substr(temp, 1, nchar(temp)-3), "tml")
    meets[i] <- temp
  }
  
  # Return
  return(meets)
}

# Wrap runnerscrape function
getRunner <- function(url) {
  runner <- tryCatch(
    {
      tempRunner <- runnerScrape(url)
      return(tempRunner)
    },
    # Error and warning handling
    error = function(cond) {
      message(paste("URL does not seem to exist:", url))
      message("Here's the original error message:")
      message(cond)
      # Choose a return value in case of error
      return(NA)
    }
  )
}

# Function to get indoor meet runner URLs
getIndoorRunnerURLs <- function(url) {
  
  # Check URL validity
  if(class(try(url %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html())) == 'try-error') {
    print(paste0("Failed to get data for : ", url))
    return(NA)
  }
  
  # Query HTML
  eventLinks <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tbody/tr/td/a") %>%
    html_attr("href")
  
  # Filter out select URLs
  # eventLinks <- eventLinks[grepl("results", eventLinks) & grepl("1_Mile|800|400m|3000|5000|10000|600|3,000m|1,000m|5,000m|1,500m|1500m|3,000mS|
  #                                                               |10,000|3000_M_S|1500", eventLinks)]
  eventLinks <- eventLinks[grepl("results", eventLinks)]
  # Create empty vector for runner URLs
  runnerURLs <- vector()
  
  # Iterate over links to fix their formats
  for (i in 1:length(eventLinks)) {
    temp <- eventLinks[i]
    #temp = substring(temp, 3)
    temp <- paste0("https:", temp)
    temp <- paste0(substr(temp, 1, nchar(temp)-3), "tml")
    eventLinks[i] <- temp
  }
  
  # Iterate over event links to get runners
  for (i in 1:length(eventLinks)) {
    # Try and get HTML
    runnerURLs <- append(runnerURLs, getMeetLinks(url = eventLinks[i]))
  }
  
  # Filter out teams and dups
  runnerURLs <- runnerURLs[grepl("athletes", runnerURLs)]
  runnerURLs <- unique(runnerURLs)
  
  return(runnerURLs)
}

# Function to get indoor meet runner URLs
getXCRunnerURLs <- function(url) {
  
  # Check URL validity
  if(class(try(url %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html())) == 'try-error') {
    print(paste0("Failed to get data for : ", url))
    return(NA)
  }
  
  # Query HTML
  pageLinks <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tbody/tr/td/div/a") %>%
    html_attr("href")
  
  runner_links <- pageLinks[grepl("athletes", pageLinks)]
  # Create empty vector for runner URLs
  runnerURLs <- vector()
  
  # Iterate over links to fix their formats
  for (i in 1:length(runner_links)) {
    temp <- runner_links[i]
    #temp = substring(temp, 3)
    temp <- paste0("https:", temp)
    temp <- paste0(substr(temp, 1, nchar(temp)-3), "tml")
    runner_links[i] <- temp
  }
  
  runnerURLs <- unique(runnerURLs)
  
  return(runnerURLs)
}

# Meet results query (XC)
xcMeetResQuery <- function(meetURL){
  # Get runner URLs
  runnerLinks <- getRunnerURLs(meetURL)
  
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
  cl <- makeCluster(cores[1] - 1, outfile = '/Users/samivanecky/git/TrackPowerRankings/scraperErrors.txt')
  registerDoParallel(cl)
  
  runner_lines <- foreach(i=1:length(runnerLinks), .combine = rbind) %dopar% {
    
    source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/meetScrapingFxns.R")
    
    # Make function call
    runner_temp <- getRunner(runnerLinks[i])
    
    runner_temp
  }
  
  stopCluster(cl)
  
  # Return data
  return(runner_lines)
}

# Indoor meet results query
trackRunnerURLQuery <- function(meetUrls) {
  
  # Detect cores
  cores <- detectCores()
  cl <- makeCluster(cores[1] - 1, outfile = '/Users/samivanecky/git/TrackPowerRankings/scraperErrors.txt')
  registerDoParallel(cl)
  
  runnerLinks <- foreach(i=1:length(meetUrls), .combine = append) %dopar% {
    
    source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
    
    tryCatch({
      # Get runner
      tempRunnerLinks <- getIndoorRunnerURLs(meetUrls[i])
      # Return value
      return(tempRunnerLinks)
    },  
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
      # Sys.sleep(60)
      return(NA)
    }
    )
  }
  
  stopCluster(cl)
  
  # Return data
  return(runnerLinks)
}

# Indoor meet results query
indoorMeetResQuery <- function(runnerLinks) {
  
  # Create a temporary dataframe for runner line item performance
  runner_lines <- as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division", FALSE, "1"))
  # Rename columns
  names(runner_lines) <- c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION", "IS_FIELD", "MARK_TIME")
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
  cl <- makeCluster(cores[1] - 1, outfile = '/Users/samivanecky/git/TrackPowerRankings/scraperErrors.txt')
  registerDoParallel(cl)
  
  runner_lines <- foreach(i=1:length(runnerLinks), .combine = rbind) %dopar% {
    
    source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
    
    tryCatch({
      # Get runner
      tempRunner <- getRunner(runnerLinks[i])
      # Return value
      return(tempRunner)
    },  
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
      # Sys.sleep(60)
      return(NA)
    }
    )
  }
  
  stopCluster(cl)
  
  # Return data
  return(runner_lines)
}

# Function to get team URL from TFRRS page
getTeamLink <- function(rnrLink) {
  teamUrl <- tryCatch(
    {
      print(paste0("Getting data for :", rnrLink))
      # Navigate to single runner
      rnrHtml <- rnrLink %>%
        GET(., timeout(30), user_agent(randUsrAgnt())) %>%
        read_html() %>%
        html_node(xpath = "/html/body/form/div/div/div/div[1]/a[3]") %>%
        html_attr("href")
      
      # Get team URL
      teamUrl <- paste0("https:", rnrHtml)
    },
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
    }
  )
}

# Function to get team roster as DF
getTeamRosterDf <- function(teamUrl) {
  teamRoster <- tryCatch(
    {
      # Get team page html
      tpHtml <- teamUrl %>%
        GET(., timeout(30), user_agent(randUsrAgnt())) %>%
        read_html() %>%
        html_table()
      
      # Get roster
      teamRoster <- as.data.frame(tpHtml[2])
      
    },
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
    }
  )
}

# Function to create a calendar based on starting and ending dates
genCal <- function(startDate = "2012-01-01") {
  # Create calendar
  cal <- as.data.frame(seq(lubridate::ymd(startDate), lubridate::today(), by = "day"))
  # Rename columns
  names(cal) <- c("cal_d")
  # Create additional variables
  cal <- cal %>%
    mutate(
      # Combination of week & year for grouping purposes
      week_key = paste0(lubridate::year(cal_d), "-", lubridate::week(cal_d)),
      month_key = paste0(lubridate::year(cal_d), "-", lubridate::month(cal_d)),
      year_index = lubridate::year(cal_d),
      month_index = lubridate::month(cal_d),
      week_index = lubridate::week(cal_d)
    )
  # Return
  return(cal)
}
