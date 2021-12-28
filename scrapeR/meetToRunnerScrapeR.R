# Code to generate line item performances from TFRRS.
# Parallel processing code to convert XC meet links into runner results

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

# Set system variables
# Sys.setenv("http_proxy"="")
# Sys.setenv("no_proxy"=TRUE)
# Sys.setenv("no_proxy"=1)

# Load functions for scraping
source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/TrackPowerRankings/scrapeR//ResultsQuery.R")
source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/meetScrapingFxns.R")

# Connect to AWS
# Read connection data from yaml
aws.yml <- read_yaml("aws.yaml")

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
# dbRemoveTable(aws, "runners_grouped")
# dbCreateTable(aws, "meet_links", linksDf)
dbWriteTable(aws, "meet_links", joinLinks, append = TRUE)

# Convert back to vector
joinLinks <- joinLinks$links

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

# Iterate over meets and get data
for (i in 1:length(joinLinks)) {
  # Check url
  tempURL <- gsub("[[:space:]]", "", joinLinks[i])
  
  # Check URL validity
  if(tempURL %>% GET(., timeout(30), user_agent(randUsrAgnt())) %>% http_error()) {
    print(paste0("Failed to get data for : ", tempURL))
    next
  }
  
  # Call query function
  meetResults <- xcMeetResQuery(tempURL)

  # Bind to existing data
  runner_lines <- rbind(runner_lines, meetResults)
  
  # Sleep for a sec
  Sys.sleep(90)
}

# Pull current data out of table
currentData <- dbGetQuery(aws, "select * from race_results")

# Add load date to all records being uploaded
runRecs <- runner_lines %>%
  mutate(
    load_d = lubridate::today()
  ) %>%
  filter(MEET_NAME != "meet") %>%
  funique() %>%
  mutate(
    TIME = as.numeric(TIME),
    PLACE = as.numeric(PLACE),
    MEET_DATE = lubridate::ymd(MEET_DATE),
    NAME = gsub("[^\x01-\x7F]", "", NAME)
  ) %>%
  mutate(
    RUNNER_KEY = paste0(NAME, "-", GENDER, "-", "TEAM")
  )

# Remove runners from current data that are in the just pulled data
currentData <- currentData %>%
 filter(!(RUNNER_KEY %in% runRecs$RUNNER_KEY))

# Join data from old & new
uploadData <- rbind(runRecs, currentData)

# Upload runner data to table
# Write data to table for URLs
# dbRemoveTable(aws, "runners_grouped")
# dbCreateTable(aws, "race_results", runRecs)
dbWriteTable(aws, "race_results", uploadData, overwrite = TRUE)


