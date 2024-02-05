# Code to extract track event results from their URLs
# This loads results to a table, utilizing the links from the track_scraped_links table
# See track_event_links_historical_load.R for loading all historical data

library(tidyverse)
library(httr)
library(RPostgreSQL)
library(DBI)
library(stringr)
library(stringi)
library(yaml)
library(rvest)
library(dplyr)
library(lubridate)
library(glue)

# Source file for functions
source("/Users/samivanecky/git/runneR//scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/altConversinFxns.R")

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

# Load data from event links
evnt_links <- dbGetQuery(pg, "select * from track_scraped_links")

# Get already scraped links
cmp_links <- dbGetQuery(pg, "select * from track_scraped_links_comp")

# Remove any links which have been scraped
evnt_links <- evnt_links %>%
  filter(
    !(link %in% cmp_links$link)
  )

# Get only 10k most recent
evnt_links <- evnt_links %>%
  slice_head(
    n = 10000
  )

# Convert to vector of links
lnks <- unique(evnt_links$link)

# Function for running in parallel
get_track_res_in_par <- function(url) {
  # Print status
  # print(paste0("Getting data for: ", url))
  
  # Attempt to get data
  tmp_res <- tryCatch(
    {
      # Get event results
      tmp_res <- getEventResults(url)
      
    }, error=function(cond) {
      message("Here's the original error message:")
      message(cond)
      return(NA)
    }
  )
  
  # Check for error
  if(!any(is.na(tmp_res))) {
    # Append data
    if(exists("all_res")) {
      all_res <- plyr::rbind.fill(all_res, tmp_res)
    } else {
      all_res <- tmp_res
    }
  } else {
    return(NULL)
  }
  
  # Return data
  return(all_res)
  
}

for (i in 1:length(lnks)) {
  # Print status
  # print(glue("Getting data for {lnks[i]}, which is link {i} of {length(lnks)}"))
  
  # Call function
  tmp_df <- get_track_res_in_par(lnks[i])
  
  # Combine data
  if(!any(is.na(tmp_df))) {
    # Append data
    if(exists("track_res")) {
      track_res <- plyr::rbind.fill(track_res, tmp_df)
    } else {
      track_res <- tmp_df
    }
  }
}

# Write data to dataframe
track_res <- as.data.frame(track_res)

# Create and write table
# dbCreateTable(pg, "ind_track_results_raw", track_res)
dbWriteTable(pg, "ind_track_results_raw", track_res, append = TRUE)

# Create and write to table for scraped links
# dbCreateTable(pg, "track_scraped_links_comp", evnt_links)
dbWriteTable(pg, "track_scraped_links_comp", evnt_links, append = TRUE)
