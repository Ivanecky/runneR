# RUN THIS FOR TRACK MEET SCRAPING IN PARALLEL
# Code to generate line item performances from TFRRS in parallel process.

# Load libraries
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

# Connect to postgres
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

# Vector to hold links that threw an error
meetErrLinks <- vector()

# Iterate over meets and get data
for (i in 1:(length(joinLinks))) {
  # Check url
  tempURL <- joinLinks[i]
  
  # Check URL validity
  if(class(try(tempURL %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html()))[1] == 'try-error') {
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
          meetErrLinks <- append(meetErrLinks, joinLinks[i])
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
runner_lines <- runnerResQueryV2(runnerLinks)

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
currentData <- dbGetQuery(pg, "select * from runner_line_item_raw") %>%
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
