# Code to generate line item performances from TFRRS for XC meets
library(tidyverse)
library(httr)
library(jsonlite)
library(RPostgreSQL)
library(DBI)
library(RSQLite)
library(reshape2)
library(stringr)
library(stringi)
library(yaml)
library(rvest)
library(kit)
library(zoo)
library(dplyr)
library(lubridate)
library(yaml)

# Source file for functions
source("/Users/samivanecky/git/runneR//scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/altConversinFxns.R")

# Meet results URL
url <- "https://www.tfrrs.org/results_search.html"

# Read connection data from yaml
pg.yml <- read_yaml("/Users/samivanecky/git/postgres.yaml")

# Connect to database
pg <- dbConnect(
  RPostgres::Postgres(),
  host = pg.yml$host,
  user = pg.yml$user,
  db = pg.yml$database,
  port = pg.yml$port
)

# Get already scraped links
scraped_links <- dbGetQuery(pg, "select * from meet_links")

# Subset
meet_links <- scraped_links$link

# Subset XC links
xc_links <- meet_links[grepl("xc", meet_links, ignore.case = T)]

# Create team data frame
teams <- as.data.frame(cbind("name", "div", "gender"))
names(teams) <- c("name", "div", "gender")

# Vector to hold team links
team_links <- c()

# Scrape results
for (i in 1:length(xc_links)) {
  # Print status
  print(paste0("Getting data for: ", xc_links[i]))
  
  # Get team links
  temp_links <- getTeamLinks(xc_links[i])
  
  # Append data
  team_links <- append(team_links, temp_links)
  team_links <- funique(team_links)
  
}

# Iterate over team links to get divisions
for (i in 1:length(team_links)) {
  print(paste0("Getting data for ", i, " out of ", length(team_links))) 
  
  # Get data
  temp <- tryCatch({
    getTeamDiv(team_links[i])
  }, 
  error=function(cond) {
    message("Here's the original error message:")
    message(cond)
    # Sys.sleep(60)
    return(NA)
  })
  
  # Combine with data
  teams <- rbind(teams, temp)
}

# Filter to unique
teams <- teams %>%
  funique()

# Upload to data frame
# dbCreateTable(pg, "team_divs", teams)
dbWriteTable(pg, "team_divs", teams, overwrite = TRUE)
