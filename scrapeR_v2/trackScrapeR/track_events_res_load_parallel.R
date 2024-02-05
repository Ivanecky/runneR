# Code to extract track event results from their URLs
# This loads results to a table, utilizing the links from the track_scraped_links table
# See track_event_links_historical_load.R for loading all historical data

library(tidyverse)
library(httr)
library(jsonlite)
library(RPostgreSQL)
library(DBI)
library(RSQLite)
library(stringr)
library(stringi)
library(yaml)
library(rvest)
library(kit)
library(zoo)
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

# Convert to vector of links
lnks <- unique(evnt_links$link)
lnks <- lnks[1:(length(lnks)/8)]

# Function for running in parallel
get_track_res_in_par <- function(url) {
  # Print status
  print(paste0("Getting data for: ", url))
  
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

# Code to execute query for backfill in parallel

# Detect cores
cores <- detectCores()
cl <- makeCluster(cores[1] - 1, outfile = '/Users/samivanecky/git/runneR/scrapeR/scraperErrors.txt')
registerDoParallel(cl)

res_lines <- foreach(i=1:length(lnks), .combine = plyr::rbind.fill, .verbose = TRUE, .inorder = FALSE) %dopar% {
  
  source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
  
  tryCatch({
    # Get runner
    tmp_res <- getEventResults(lnks[i])
    # Return value
    return(tmp_res)
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

# Write data to dataframe
track_res <- as.data.frame(track_res)

# Create and write table
# dbCreateTable(pg, "ind_track_results_raw", track_res)
dbWriteTable(pg, "ind_track_results_raw", track_res, append = TRUE)

# Create and write to table for scraped links
# dbCreateTable(pg, "track_scraped_links_comp", evnt_links)
dbWriteTable(pg, "track_scraped_links_comp", evnt_links, append = TRUE)
