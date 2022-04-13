# RUN THIS FOR TRACK MEET SCRAPING
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
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")

# Connect to AWS
# Read connection data from yaml
pg.yml <- read_yaml("/Users/samivanecky/git/runneR/postgres.yaml")

# Connect to database
pg <- dbConnect(
  RPostgres::Postgres(),
  host = pg.yml$host,
  user = pg.yml$user,
  db = pg.yml$database,
  port = pg.yml$port
)

# Meet results URL
url <- "https://www.tfrrs.org/results_search.html"

# Read meet name links
links <- getMeetLinks(url)

# Create a links DF to upload links to AWS table, storing links for meets that have been scraped
linksDf <- as.data.frame(links)

# Query links from link table
linkTbl <- dbGetQuery(pg, "select * from meet_links")

# Get new links (not in table)
joinLinks <- linksDf %>%
  filter(!(links %in% linkTbl$links))

# Write data to table for URLs
#dbRemoveTable(pg, "meet_links")
#dbCreateTable(pg, "meet_links", linksDf)
dbWriteTable(pg, "meet_links", joinLinks, append = TRUE)

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

# Query data in parallel
runner_lines <- runnerResQuery(runnerLinks)

# Reconnect to data
# Connect to database
pg <- dbConnect(
  RPostgres::Postgres(),
  host = pg.yml$host,
  user = pg.yml$user,
  db = pg.yml$database,
  port = pg.yml$port
)


# Upload to AWS database
# Pull current data out of table
currentData <- dbGetQuery(pg, "select * from results_line_item_dets") %>%
  mutate(
    RUNNER_KEY = paste0(NAME, "-", GENDER, "-", TEAM)
  ) 

# Modifying data before loading
runRecs <- runner_lines %>%
  as.data.frame() %>%
  filter(MEET_NAME != "meet") %>%
  funique() %>%
  mutate(
    PLACE = as.numeric(PLACE),
    NAME = gsub("[^\x01-\x7F]", "", NAME)
  ) %>%
  mutate(
    RUNNER_KEY = paste0(NAME, "-", GENDER, "-", TEAM)
  )

# Remove runners who are in the new data
currentData <- currentData %>%
  filter(!(RUNNER_KEY %in% runRecs$RUNNER_KEY))

# Join data from old & new
uploadData <- plyr::rbind.fill(runRecs, currentData) %>%
  funique() %>%
  mutate(
    load_d = lubridate::today(),
    MEET_DATE = gsub(",", "", MEET_DATE)
  ) %>%
  filter(EVENT != "OTHER")

# Upload runner data to table
# Write data to table for URLs
# dbRemoveTable(aws, "race_results")
# dbCreateTable(pg, "runner_line_item_raw", runRecs)
dbWriteTable(pg, "runner_line_item_raw", uploadData, overwrite = TRUE)
