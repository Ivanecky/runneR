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

# Vector to hold runner URLs
runnerLinks <- vector()

# Iterate over meets and get data
for (i in 1:(length(links)-900)) {
  # Check url
  # tempURL <- gsub("[[:space:]]", "", links[i])
  tempURL <- links[i]
  
  # Print message for meet
  print(paste0("Getting data for: ", tempURL))
  
  # Get runner URLs
  tempLinks <- getIndoorRunnerURLs(links[i])
  
  # Bind runners 
  runnerLinks <- append(runnerLinks, tempLinks)
  
}

# Get unqiue runners
runnerLinks <- funique(runnerLinks)

# Create roster DF
teamRosters <- as.data.frame(cbind("NAME", "YEAR"))
names(teamRosters) <- c("NAME", "YEAR")

# Vector to hold team links
teamLinks <- vector()

# Iterate over runner links and get team links
for (i in 1:length(runnerLinks)) {
  tempLink <- getTeamLink(runnerLinks[i])
  teamLinks <- append(teamLinks, tempLink)
}

# Get unique teams 
teamLinks <- funique(teamLinks)

# Get team rosters
for (i in 1:length(teamLinks)) {
  
  tempRoster <- getTeamRosterDf(teamLinks[i])
  
  try(
    teamRosters <- rbind(teamRosters, tempRoster)
  )
}
