# Code to generate line item performances from TFRRS.
library(tidyverse)
library(httr)
library(dplyr)
library(DBI)
library(reshape2)
library(stringr)
library(stringi)
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

getMeetLinks <- function() {
  meet_links <- c()
  
  for (i in 1:2000) {
    # Status
    print(paste0("Getting data for page ", i))
    
    # Set URL
    temp_url <- paste0("https://tfrrs.org/results_search.html?page=", i)
    
    # Read HTML data
    temp_links <- temp_url %>%
      GET(., timeout(30)) %>%
      read_html() %>%
      html_nodes(xpath = "//tr/td/a") %>%
      html_attr("href")
    
    # Check if results are returned
    if (length(temp_links) == 0) {
      break
    }
    
    # Fix formatting
    for ( i in 1:length(temp_links) )
    {
      if(!grepl("http", temp_links[i])) {
        temp <- temp_links[i]
        temp <- paste0("https://tfrrs.org", temp)
        temp <- gsub("[[:space:]]", "", temp)
        temp_links[i] <- temp
      }
    }
    
    # Append links to list
    meet_links <- append(meet_links, temp_links)
  }
  
  # Return
  return(meet_links)
}

getCurrentMeetLinks <- function() {
  # Empty vector
  meet_links <- c()
  
  # Get default home page
  url <- "https://www.tfrrs.org/results_search.html"
  
  # Read HTML data
  temp_links <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tr/td/a") %>%
    html_attr("href")
  
  # Check if results are returned
  if (length(temp_links) == 0) {
    break
  }
  
  # Fix formatting
  for ( i in 1:length(temp_links) )
  {
    if(!grepl("http", temp_links[i])) {
      temp <- temp_links[i]
      temp <- paste0("https://tfrrs.org", temp)
      temp <- gsub("[[:space:]]", "", temp)
      temp_links[i] <- temp
    }
  }
  
  # Append links to list
  meet_links <- append(meet_links, temp_links)
  
  
  for (i in 1:50) {
    # Status
    print(paste0("Getting data for page ", i))
    
    # Set URL
    temp_url <- paste0("https://tfrrs.org/results_search.html?page=", i)
    
    # Read HTML data
    temp_links <- temp_url %>%
      GET(., timeout(30)) %>%
      read_html() %>%
      html_nodes(xpath = "//tbody/tr/td/a") %>%
      html_attr("href")
    
    # Check if results are returned
    if (length(temp_links) == 0) {
      break
    }
    
    # Fix formatting
    for ( i in 1:length(temp_links) )
    {
      if(!grepl("http", temp_links[i])) {
        temp <- temp_links[i]
        temp <- paste0("https://tfrrs.org", temp)
        temp <- gsub("[[:space:]]", "", temp)
        temp_links[i] <- temp
      }
    }
    
    # Append links to list
    meet_links <- append(meet_links, temp_links)
  }
  
  # Return
  return(meet_links)
}

getPLMeetLinks <- function(url) {
  
  if(class(try(url %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html()))[1] == 'try-error') {
    print(paste0("Failed to get data for : ", url))
    return(NA)
  }
  
  # Get runner links
  meets <- url %>%
    GET(., timeout(30), user_agent(randUsrAgnt())) %>%
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
    if(!grepl("http", meets[i])) {
      temp <- meets[i]
      temp <- paste0("https:", temp)
      temp <- gsub("[[:space:]]", "", temp)
      # temp <- paste0(substr(temp, 1, nchar(temp)-3), "tml")
      meets[i] <- temp
    }
  }
  
  # Return
  return(meets)
}

# Wrap runnerscrape function
getRunner <- function(url) {
  runner <- tryCatch(
    {
      tempRunner <- runnerScrapeV2(url)
      
      # Verify result is a data frame
      if(is.data.frame(tempRunner)) {
        return(tempRunner)
      } else {
        return(NA)
      }
      
    },
    # Error and warning handling
    error = function(cond) {
      # Choose a return value in case of error
      print(paste0("Error getting data for: ", url))
      return(NA)
    }
  )
}

# Parallel get runner
getParRunner <- function(url) {
  runner <- tryCatch(
    {
      # Get runner data
      tempRunner <- runnerScrapeV2(url)
      
      # Add upload date
      tempRunner$load_d = lubridate::today()
      
      # Connect to database
      pg <- dbConnect(
        RPostgres::Postgres(),
        host = 'localhost',
        user = 'samivanecky',
        db = 'runner',
        port = 5432
      )
      
      # Upload to postgres
      dbWriteTable(pg, "runner_lines_stg", tempRunner, append = TRUE)
      
      # Create string
      retVal <- paste0("SUCCESS: ", url)
      
      # Return value to hold in list
      return(retVal)
    },
    # Error and warning handling
    error = function(cond) {
      # Choose a return value in case of error
      print(paste0("Error getting data for: ", url))
      # Set return value
      retVal <- paste0("ERROR: ", url)
      # Return URL of runner with error
      return(retVal)
    }
  )
}

# Function to get indoor meet runner URLs
getIndoorRunnerURLs <- function(url) {
  
  # Check URL validity
  if(class(try(url %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html()))[1] == 'try-error') {
    print(paste0("Failed to get data for : ", url))
    return(NA)
  }
  
  # Query HTML
  eventLinks <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tbody/tr/td/a") %>%
    html_attr("href")
  
  # Subset links
  eventLinks <- eventLinks[grepl("results", eventLinks)]
  # Create empty vector for runner URLs
  runnerURLs <- vector()
  
  # Iterate over links to fix their formats
  for (i in 1:length(eventLinks)) {
    if(!grepl("https", eventLinks[i])) {
      temp <- eventLinks[i]
      #temp = substring(temp, 3)
      temp <- paste0("https:", temp)
      temp <- paste0(substr(temp, 1, nchar(temp)-3), "tml")
      eventLinks[i] <- temp
    }
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
               read_html()))[1] == 'try-error') {
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
runnerResQuery <- function(runnerLinks) {
  
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
  cl <- makeCluster(cores[1] - 1, outfile = '/Users/samivanecky/git/runneR/scrapeR/scraperErrors.txt')
  registerDoParallel(cl)
  
  runner_lines <- foreach(i=1:length(runnerLinks), .combine = plyr::rbind.fill, .errorhandling = "remove", .verbose = TRUE, .inorder = FALSE, .init = runner_lines) %dopar% {
    
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

# Indoor meet results query
runnerResQueryV2 <- function(runnerLinks) {
  
  # # Create a temporary dataframe for runner line item performance
  # runner_lines_res <- as.data.frame(cbind("result"))
  # # Rename columns
  # names(runner_lines_res) <- c("RESULT")
  
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
  cl <- makeCluster(cores[1], outfile = '/Users/samivanecky/git/runneR/scrapeR/scraperErrors.txt')
  registerDoParallel(cl)
  
  runner_lines <- foreach(i=1:length(runnerLinks), .combine = rbind, .errorhandling = "remove", .verbose = TRUE, .inorder = FALSE, .init = runner_lines) %dopar% {
    
    source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
    
    # Get runner
    tempRunner <- getRunner(runnerLinks[i])
    
    # Return value
    return(tempRunner)
    
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
genCal <- function(startDate = "2009-01-01") {
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

# Function to get meet runner URLs in parallel
getParRunnerURLs <- function(meetLinks) {
  
  # Detect cores
  cores <- detectCores()
  cl <- makeCluster(cores[1]-1, methods = FALSE)
  registerDoParallel(cl)
  
  runnerLinks <- foreach(i=1:length(meetLinks), .combine = c, .errorhandling = "remove", .verbose = TRUE, .inorder = FALSE) %dopar% {
    
    source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
    
    # Get runner URLs
    tryCatch({
      # Get runner
      tempRunnerLinks <- getIndoorRunnerURLs(meetLinks[i])
      # Return value
      return(tempRunnerLinks)
    },  
    error=function(cond) {
        tempRunnerLinks <- getXCRunnerURLs(meetLinks[i])
        # Return value
        return(tempRunnerLinks)
      }
    )
  }
  
  # Stop cluster
  stopCluster(cl)
  
  # Return data
  return(runnerLinks)
}

# Function to get event results
getEventResults <- function(url) {
  # Get html
  html <- url %>%
    read_html()
  
  tabs <- html %>%
    html_table()
  
  meet_name <- html %>%
    html_element(xpath = "/html/body/div[3]/div/div/div[1]/h3/a") %>%
    html_text()
  
  meet_date <- html %>%
    html_element(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[1]") %>%
    html_text()
  
  meet_loc <- html %>%
    html_element(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[3]") %>%
    html_text()
  
  race_names <- html %>%
    html_elements(css = "h3") %>%
    html_text()
  
  # Clean up whitespace
  meet_name <- trimws(meet_name)
  meet_date <- trimws(meet_date)
  meet_loc <- trimws(meet_loc)
  race_names <- trimws(race_names)
  
  # Drop first race name (same as meet name)
  race_names <- race_names[2:length(race_names)]
  
  # Read tables
  for (i in 1:length(tabs)) {
    if(exists("race_results")) {
      # Create temporary holding table
      temp_results <- as.data.frame(tabs[i])
      # Subset data
      temp_results <- temp_results %>%
        select(
          PL, NAME, YEAR, TEAM, TIME
        )
      # Add race name
      temp_results$RACE_NAME <- race_names[2]
      # Bind to existing data
      race_results <- plyr::rbind.fill(race_results, temp_results)
      
    } else {
      # Create new table
      race_results <- as.data.frame(tabs[i])
      # Subset data
      race_results <- race_results %>%
        select(
          PL, NAME, YEAR, TEAM, TIME
        )
      # Add race name
      race_results$RACE_NAME <- race_names[1]
    }
  }
  
  # Append other fields to table
  race_results <- race_results %>%
    mutate(
      MEET_DATE = meet_date,
      MEET_LOCATION = meet_loc,
      MEET_NAME = meet_name
    ) %>%
    unique()
  
  # Return results
  return(race_results)
}

# Function to get event links
getEventLinks <- function(url) {
  # Check URL validity
  if(class(try(url %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html()))[1] == 'try-error') {
    print(paste0("Failed to get data for : ", url))
    return(NA)
  }
  
  # Query HTML
  eventLinks <- url %>%
    GET(., timeout(30)) %>%
    read_html() %>%
    html_nodes(xpath = "//tbody/tr/td/a") %>%
    html_attr("href")
  
  # Subset links
  eventLinks <- eventLinks[grepl("results", eventLinks)]
  
  # Return links
  return(eventLinks)
}

getEventLinksPar <- function(links) {
  # Detect cores
  cores <- detectCores()
  cl <- makeCluster(cores[1], outfile = '/Users/samivanecky/git/runneR/scrapeR/scraperErrors.txt')
  registerDoParallel(cl)
  
  event_links <- vector()
  
  event_links <- foreach(i=1:length(links), .combine = c, .errorhandling = "remove", .verbose = TRUE, .inorder = FALSE, .init = event_links) %dopar% {
    
    # Source file
    source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
    
    # Check URL validity
    if(class(try(links[i] %>%
                 GET(., timeout(30), user_agent(randUsrAgnt())) %>%
                 read_html()))[1] == 'try-error') {
      next
    }
    
    # Get runner URLs
    tempEventLinks <- tryCatch({
      # Get runner
      tempEventLinks <- getEventLinks(links[i])
    },  
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
      # Sys.sleep(60)
      return(NA)
    })
    
    # Return value
    return(tempEventLinks)
    
  }
  
  stopCluster(cl)
  
  # Return data
  return(event_links)
}

# Get meet results in parallel
getTrackMeetResPar <- function(links) {
  # Init DF
  # Create a temporary dataframe for runner line item performance
  meet_res <- as.data.frame(cbind(-1, "name", "year", "team", -999, "race name", "meet dt", "meet loc", "meet name"))
  # Rename columns
  names(meet_res) <- c("PL", "NAME", "YEAR", "TEAM", "TIME", "RACE_NAME", "MEET_DATE", "MEET_LOCATION", "MEET_NAME")
  
  # Detect cores
  cores <- detectCores()
  cl <- makeCluster(cores[1], outfile = '/Users/samivanecky/git/runneR/scrapeR/scraperErrors.txt')
  registerDoParallel(cl)
  
  # Parallel code
  meet_res <- foreach(i=1:length(links), .combine = rbind, .errorhandling = "remove", .verbose = TRUE, .inorder = FALSE, .init = meet_res) %dopar% {
    
    # Source file
    source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
    
    # Try and get results
    temp_res <- tryCatch({
        getEventResults(links[i])
      }, 
      error=function(cond) {
        message("Here's the original error message:")
        message(cond)
        # Sys.sleep(60)
        return(NA)
      })
    
    # Return value
    return(temp_res)
  }
  
  stopCluster(cl)
  
  # Return data
  return(meet_res)
}

# Get meet results in parallel
getTrackMeetInfoPar <- function(links) {
  # Init DF
  # Create a temporary dataframe for runner line item performance
  meet_info <- as.data.frame(cbind("meet_name", "meet_date", "meet_fac", "meet_trk_sz"))
  # Rename columns
  names(meet_info) <- c("MEET_NAME", "MEET_DATE", "MEET_FACILITY", "MEET_TRACK_SZ")
  # Convert columns
  meet_info <- meet_info %>%
    mutate(
      MEET_NAME = as.character(MEET_NAME),
      MEET_DATE = as.character(MEET_DATE),
      MEET_FACILITY = as.character(MEET_FACILITY),
      MEET_TRACK_SZ = as.character(MEET_TRACK_SZ)
    )
  
  # Detect cores
  cores <- detectCores()
  cl <- makeCluster(cores[1], outfile = '/Users/samivanecky/git/runneR/scrapeR/scraperErrors.txt')
  registerDoParallel(cl)
  
  # Parallel code
  meet_info <- foreach(i=1:length(links), .combine = rbind, .errorhandling = "remove", .verbose = TRUE, .inorder = FALSE, .init = meet_info) %dopar% {
    
    # Source file
    source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
    
    # Try and get result
    temp_info <- tryCatch({
      getTrackMeetInfo(links[i])
    }, 
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
      # Sys.sleep(60)
      return(NA)
    })
    
    # Return value
    return(temp_info)
  }
  
  stopCluster(cl)
  
  # Return data
  return(meet_info)
}

getTrackMeetInfo <- function(link) {
  # Go through meets and get dates
  tempUrl = link
  
  if(class(try(tempUrl %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html()))[1] == 'try-error') {
    print(paste0("Failed to get data for : ", tempUrl)) 
    errorLinks <- append(errorLinks, tempUrl)
    next
  }
  
  # Get html txt
  tempHtml <- tempUrl %>%
    GET(., timeout(30)) %>%
    read_html() 
  
  tempFacilityTxt <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[3]") %>%
    html_text()
  
  tempTrackSize <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[4]") %>%
    html_text()
  
  tempTxt <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[1]") %>%
    html_text()
  
  tempMeetName <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[1]/h3") %>%
    html_text()
  
  # Drop new line char, etc
  tempTxt <- gsub("[[:space:]]", "", tempTxt)
  
  # Get year
  tempYr <- case_when(
    grepl('2005', tempTxt) ~ '2005',
    grepl('2006', tempTxt) ~ '2006',
    grepl('2007', tempTxt) ~ '2007',
    grepl('2008', tempTxt) ~ '2008',
    grepl('2009', tempTxt) ~ '2009',
    grepl('2010', tempTxt) ~ '2010',
    grepl('2011', tempTxt) ~ '2011',
    grepl('2012', tempTxt) ~ '2012',
    grepl('2013', tempTxt) ~ '2013',
    grepl('2014', tempTxt) ~ '2014',
    grepl('2015', tempTxt) ~ '2015',
    grepl('2016', tempTxt) ~ '2016',
    grepl('2017', tempTxt) ~ '2017',
    grepl('2018', tempTxt) ~ '2018',
    grepl('2019', tempTxt) ~ '2019',
    grepl('2020', tempTxt) ~ '2020',
    grepl('2021', tempTxt) ~ '2021',
    grepl('2022', tempTxt) ~ '2022',
    grepl('2023', tempTxt) ~ '2023',
    T ~ 'OTHER'
  )
  
  # Get day number
  tempDay <- stri_extract_first_regex(tempTxt, "[0-9]+")
  
  # Get month
  tempMon <- case_when(
    grepl("Jan", tempTxt) ~ '1',
    grepl('Feb', tempTxt) ~ '2',
    grepl('Mar', tempTxt) ~ '3',
    grepl("Apr", tempTxt) ~ '4',
    grepl('May', tempTxt) ~ '5',
    grepl('Jun', tempTxt) ~ '6',
    grepl('Jul', tempTxt) ~ '7',
    grepl('Aug', tempTxt) ~ '8',
    grepl('Sep', tempTxt) ~ '9',
    grepl('Oct', tempTxt) ~ '10',
    grepl('Nov', tempTxt) ~ '11',
    grepl('Dec', tempTxt) ~ '12',
    T ~ '0'
  )
  
  # Combine into date
  tempDt <- paste0(tempYr, "-", tempMon, "-", tempDay)
  
  # Check for NULLs
  tempFacilityTxt <- ifelse(identical(tempFacilityTxt, character(0)), "no location", tempFacilityTxt)
  
  tempTrackSize <- ifelse(identical(tempTrackSize, character(0)), "no location", tempTrackSize)
  
  # Create as dataframe
  temp_info <- as.data.frame(cbind(tempMeetName, tempDt, tempFacilityTxt, tempTrackSize))
  names(temp_info) <- c("MEET_NAME", "MEET_DATE", "MEET_FACILITY", "MEET_TRACK_SZ")
  temp_info <- temp_info %>%
    mutate(
      MEET_NAME = as.character(MEET_NAME),
      MEET_DATE = as.character(MEET_DATE),
      MEET_FACILITY = as.character(MEET_FACILITY),
      MEET_TRACK_SZ = as.character(MEET_TRACK_SZ)
    )
  
  # Return data
  return(temp_info)
}

# Get XC results
getXCResults <- function(url) {
  
  if(class(try(url %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html()))[1] == 'try-error') {
    print(paste0("Failed to get data for : ", url)) 
    break
  }
  
  # Get html in one call
  temp_html <- url %>%
    GET(., timeout(30), user_agent(randUsrAgnt())) %>%
    read_html()
  
  tbls <- temp_html %>%
    html_table()
  
  # Get headers
  hdrs <- temp_html %>%
    html_nodes("h3") %>%
    html_text()
  
  # Clean up headers
  hdrs <- gsub("[\r\n]", "", hdrs)
  hdrs <- hdrs[grepl("result", hdrs, ignore.case = T)]
  
  # Get meet name
  meet_name <- url %>%
    read_html() %>%
    html_node(xpath = "/html/body/div[3]/div/div/div[1]/h3") %>%
    html_text()
  
  # Clean up meet name
  meet_name = trimws(gsub("[\r\n]", "", meet_name))
  
  # Get meet date
  meet_dt <- temp_html %>%
    html_node(xpath = "/html/body/div[3]/div/div/div[2]/div[1]/div/div/div/div[1]") %>%
    html_text()
  
  # Drop new line char, etc
  meet_dt <- gsub("[[:space:]]", "", meet_dt)
  
  # Get year
  tempYr <- case_when(
    grepl('2005', meet_dt) ~ '2005',
    grepl('2006', meet_dt) ~ '2006',
    grepl('2007', meet_dt) ~ '2007',
    grepl('2008', meet_dt) ~ '2008',
    grepl('2009', meet_dt) ~ '2009',
    grepl('2010', meet_dt) ~ '2010',
    grepl('2011', meet_dt) ~ '2011',
    grepl('2012', meet_dt) ~ '2012',
    grepl('2013', meet_dt) ~ '2013',
    grepl('2014', meet_dt) ~ '2014',
    grepl('2015', meet_dt) ~ '2015',
    grepl('2016', meet_dt) ~ '2016',
    grepl('2017', meet_dt) ~ '2017',
    grepl('2018', meet_dt) ~ '2018',
    grepl('2019', meet_dt) ~ '2019',
    grepl('2020', meet_dt) ~ '2020',
    grepl('2021', meet_dt) ~ '2021',
    grepl('2022', meet_dt) ~ '2022',
    grepl('2023', meet_dt) ~ '2023',
    T ~ 'OTHER'
  )
  
  # Get day number
  tempDay <- stri_extract_first_regex(meet_dt, "[0-9]+")
  
  # Get month
  tempMon <- case_when(
    grepl("Jan", meet_dt) ~ '1',
    grepl('Feb', meet_dt) ~ '2',
    grepl('Mar', meet_dt) ~ '3',
    grepl("Apr", meet_dt) ~ '4',
    grepl('May', meet_dt) ~ '5',
    grepl('Jun', meet_dt) ~ '6',
    grepl('Jul', meet_dt) ~ '7',
    grepl('Aug', meet_dt) ~ '8',
    grepl('Sep', meet_dt) ~ '9',
    grepl('Oct', meet_dt) ~ '10',
    grepl('Nov', meet_dt) ~ '11',
    grepl('Dec', meet_dt) ~ '12',
    T ~ '0'
  )
  
  # Combine into date
  meet_dt <- paste0(tempYr, "-", tempMon, "-", tempDay)
  
  # Wrap this all in a loop
  for (i in 1:length(tbls)) {
    # Convert to df
    temp_df <- as.data.frame(tbls[i])
    # Check if individual or team df
    if(any("X1"== colnames(temp_df))) {
      # Team data
      if(!exists("team_df")) {
        # Subset columns
        temp_df <- temp_df %>%
          select(PL, Team, X1, X2, X3, X4, X5, X6, X7, Score, `Avg..Time`, `Total.Time`)
        # Assign race name
        temp_df$RACE_TYPE = hdrs[i]
        # Create dataframe
        team_df <- temp_df
      } else {
        # Subset columns
        temp_df <- temp_df %>%
          select(PL, Team, X1, X2, X3, X4, X5, X6, X7, Score, `Avg..Time`, `Total.Time`)
        # Assign race type
        temp_df$RACE_TYPE = hdrs[i]
        # Create dataframe
        # team_df <- rbind(team_df, temp_df)
        team_df <- rbind(team_df, temp_df)
      }
    } else {
      # Individual data
      if(!exists("ind_df")) {
        # Subset data
        temp_df <- temp_df %>%
          select(PL, NAME, TEAM, YEAR, TIME, SCORE)
        # Assign race type
        temp_df$RACE_TYPE = hdrs[i]
        # Create df
        ind_df <- temp_df
      } else {
        # Subset data
        temp_df <- temp_df %>%
          select(PL, NAME, TEAM, YEAR, TIME, SCORE)
        # Assign race type
        temp_df$RACE_TYPE = hdrs[i]
        # Create df
        # ind_df <- rbind(ind_df, temp_df)
        ind_df <- rbind(ind_df, temp_df)
      }
    }
  }
  
  # Add meet name and date to tables
  team_df$MEET_NAME = meet_name
  team_df$MEET_DT = meet_dt
  ind_df$MEET_NAME = meet_name
  ind_df$MEET_DT = meet_dt
  
  # Convert output to list
  output <- list("teams" = team_df, "individuals" = ind_df)
  
  # Return
  return(output)
  
}

# function to get team links for team info
getTeamLinks <- function(url) {
  # Select rand usr
  ru <- randUsrAgnt()
  
  if(class(try(url %>%
               GET(., timeout(30), user_agent(ru)) %>%
               read_html()))[1] == 'try-error') {
    print(paste0("Failed to get data for : ", url)) 
    break
  }
  
  # Get html in one call
  temp_html <- url %>%
    GET(., timeout(30), user_agent(ru)) %>%
    read_html()
  
  # Get all href fields
  refs <- temp_html %>%
    html_nodes(xpath = "//tr/td/a") %>%
    html_attr("href")
  
  # Subset to teams
  refs <- refs[grepl("teams", refs)]
  
  # Return URLs
  return(refs)
}

getTeamDiv <- function(url) {
  # Select rand usr
  ru <- randUsrAgnt()
  
  if(class(try(url %>%
               GET(., timeout(30), user_agent(ru)) %>%
               read_html()))[1] == 'try-error') {
    print(paste0("Failed to get data for : ", url)) 
    break
  }
  
  # Assign gender
  gender <- case_when(
    grepl('college_f', url) ~ "F",
    grepl('college_m', url) ~ "M",
    T ~ "other"
  )
  
  # Navigate to link
  temp_team <- url %>%
    GET(., timeout(30), user_agent(ru)) %>%
    read_html()
  
  # Get team name
  temp_name <- temp_team %>%
    html_node(xpath = "/html/body/form/div/div/div/div[1]/h3[1]") %>%
    html_text()
  
  # Remove whitespace 
  temp_name <- trimws(gsub("\\n|\\t", "", temp_name, ignore.case = T))
  
  # Read xpath
  temp_txt <- temp_team %>%
    html_node(xpath = "/html/body/form/div/div/div/div[2]/div[1]/div[1]/span") %>%
    html_text() %>%
    toupper()
  
  # Check division
  temp_div <- case_when(
    grepl("D1|DI|DIVISION 1", temp_txt, ignore.case = TRUE) & !grepl("D2|DII|DIVISION 2", temp_txt, ignore.case = TRUE) & !grepl("D3|DIII|DIVISION 3", temp_txt, ignore.case = TRUE) ~ "D1",
    grepl("D2|DII|DIVISION 2", temp_txt, ignore.case = TRUE) & !grepl("D3|DIII|DIVISION 3", temp_txt, ignore.case = TRUE) ~ "D2",
    grepl("D3|DIII|DIVISION 3", temp_txt, ignore.case = TRUE) ~ "D3",
    grepl("NAIA", temp_txt, ignore.case = TRUE) ~ "NAIA",
    grepl("NJCAA", temp_txt, ignore.case = TRUE) ~ "NJCAA",
    T ~ "other"
  )
  
  # Bind in temp df
  temp_df <- as.data.frame(cbind(temp_name, temp_div, gender))
  names(temp_df) <- c("name", "div", "gender")
  
  # Return the data
  return(temp_df)
}
