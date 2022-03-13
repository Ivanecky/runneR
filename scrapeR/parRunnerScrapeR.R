# RUN THIS FOR INDOOR MEET SCRAPING
# Code to generate line item performances from TFRRS in non-parallel process.
# Code uses the TF meet scraping function to get runner links. Identical code for
# XC can be found in the nonParXCScrapeR.R file
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
library(kit)
library(RCurl)

# Load functions for scraping
# source("/Users/samivanecky/git/runneR/scrapeR/ResultsQuery.R")
# source("/Users/samivanecky/git/runneR/scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")

# Connect to AWS
# Read connection data from yaml
aws.yml <- read_yaml("/Users/samivanecky/git/TrackPowerRankings/aws.yaml")

# Connect to database
aws <- dbConnect(
  RPostgres::Postgres(),
  host = aws.yml$host,
  user = aws.yml$user,
  password = aws.yml$password,
  port = aws.yml$port
)

# Test URL
url <- "https://www.tfrrs.org/results_search.html"

# Read meet name links
links <- getMeetLinks(url)

# Create a links DF to upload links to AWS table, storing links for meets that have been scraped
linksDf <- as.data.frame(links)

# Query links from link table
linkTbl <- dbGetQuery(aws, "select * from meet_links")

# Get new links (not in table)
joinLinks <- linksDf %>%
  filter(!(links %in% linkTbl$links))

# Write data to table for URLs
# dbRemoveTable(aws, "meet_links")
# dbCreateTable(aws, "meet_links", linksDf)
dbWriteTable(aws, "meet_links", joinLinks, append = TRUE)

# Convert back to vector
joinLinks <- joinLinks$links

# Vector to hold runner URLs
runnerLinks <- vector()

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

# Iterate over meets and get data
for (i in 1:(length(joinLinks))) {
  # Check url
  tempURL <- joinLinks[i]
  
  # Check URL validity
  if(class(try(tempURL %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html())) == 'try-error') {
    print(paste0("Failed to get data for : ", tempURL))
    next
  }
  
  # Print message for meet
  print(paste0("Getting data for ", i, " out of ", length(joinLinks)))
  
  # Get runner URLs
  tryCatch({
        # Get runner
        tempRunnerLinks <- getIndoorRunnerURLs(joinLinks[i])
        # Return value
        return(tempRunnerLinks)
      },  
      error=function(cond) {
        tryCatch({
          # Get runner
          tempRunnerLinks <- getXCRunnerURLs(joinLinks[i])
          # Return value
          return(tempRunnerLinks)
        },  
        error=function(cond) {
          message("Here's the original error message:")
          message(cond)
          # Sys.sleep(60)
          return(NA)
        })
      })
  
  # Bind runners 
  runnerLinks <- append(runnerLinks, tempRunnerLinks)
  
}

# Get unqiue runners
runnerLinks <- funique(runnerLinks)

# Get runner data
# Error links
errorLinks <- vector()

# TEST CODE
# testDf <- indoorMeetResQuery(runnerLinks[1:1000])

# Iterate over all the results and get runner results
for (i in 1:(ceiling(length(runnerLinks) / 1000))) {
  # Get results for i to i+1000
  # Set boundaries
  lowBound <- (1 + (1000 * (i-1)))
  upBound <- case_when(
    (1000 * i) > length(runnerLinks) ~ as.numeric(length(runnerLinks)),
    T ~ (1000 * i)
  )
  
  # Print for tracking
  print(paste0("Getting data from ", lowBound, " to ", upBound))

  # Get results
  tempDf <- indoorMeetResQuery(runnerLinks[lowBound:upBound])
  
  # Append to dataframe
  runner_lines <- rbind(runner_lines, tempDf)
}
