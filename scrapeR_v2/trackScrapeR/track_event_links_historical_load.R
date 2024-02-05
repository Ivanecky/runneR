# Code to extract track event URLs from meet links for indoor & outdoor
# This does a full historical load of the event URLs across all stored meets in the meet_links database
# See track_event_links_current_load.R for loading only new data

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
pg.yml <- read_yaml("~/git/runneR/secrets/aws_rds.yaml")

# Connect to database
pg <- dbConnect(
  RPostgres::Postgres(),
  host = pg.yml$host,
  user = pg.yml$user,
  db = pg.yml$dbname,
  port = pg.yml$port,
  password = pg.yml$pwd
)

# Get already scraped links
meet_links <- dbGetQuery(pg, "select * from meet_links")

# Convert to df
meet_links <- as.data.frame(meet_links)
names(meet_links) <- c("link")

# Fix link double backslashes
meet_links$link <- gsub("org//", "org/", meet_links$link)

# Remove dups
meet_links <- funique(meet_links)

# Convert back to vector
meet_links <- meet_links$link

# Subset XC links
track_links <- meet_links[!(grepl("xc", meet_links, ignore.case = T))]

# Error URLs
err_urls <- c()

# Scrape results
for (i in 1:length(track_links)) {
  # Print status
  print(paste0("Getting data for: ", track_links[i], " which is ", i, " of ", length(track_links)))
  # Grab links to event results for the given meet
  tmp_evnt_links <- tryCatch({
    getEventLinks(track_links[i])
  }, 
  error=function(cond) {
    message("Here's the original error message:")
    message(cond)
    err_urls <- append(err_urls, track_links[i])
    # Sys.sleep(60)
    return(NA)
  })
  # Check for error
  if(!any(is.na(tmp_evnt_links))) {
    # Append data
    if(exists("event_links")) {
      event_links <- append(event_links, tmp_evnt_links)
    } else {
      event_links <- tmp_evnt_links
    }
  }
}

# Convert event links to table, add load date and filter to unique
event_link_tbl <- as.data.frame(event_links)
event_link_tbl <- event_link_tbl %>%
  rename(
    "link" = event_links
  ) %>%
  mutate(
    load_d = lubridate::today()
  ) %>%
  funique()

# Convert back to dataframe
event_link_tbl <- as.data.frame(event_link_tbl)

# Write to tables
# dbCreateTable(pg, "track_scraped_links", event_link_tbl, append = TRUE)
dbWriteTable(pg, "track_scraped_links", event_link_tbl, append = TRUE)

