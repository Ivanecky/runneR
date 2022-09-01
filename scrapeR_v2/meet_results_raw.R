# Code to generate line item performances from TFRRS.
library(tidymodels)
library(httr)
library(jsonlite)
library(RPostgreSQL)
library(DBI)
library(RSQLite)
library(reshape2)
library(stringr)
library(yaml)
library(rvest)
library(kit)
library(zoo)
library(dplyr)
library(lubridate)
library(fastverse)
library(tidytable)
library(data.table)
library(plyr)
library(yaml)

# Source file for functions
source("/Users/samivanecky/git/runneR//scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/altConversinFxns.R")

# Meet results URL
url <- "https://www.tfrrs.org/results_search.html"

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

# Read meet name links
meetLinks <- getMeetLinks(url)

# Query old meet links
# meetLinks <- dbGetQuery(pg, "select distinct * from meet_links")

# Remove any dups
meetLinks <- funique(meetLinks)
meetLinks <- meetLinks$links

# Vector to hold links that threw an error
meetErrLinks <- vector()

# Parallel function
event_links <- getEventLinksPar(meetLinks)

# Remove any duplicates
event_links <- funique(event_links)

# Iterate over events and get data
for (i in 1:length(event_links)) {
  # Set default value
  skip_ <- FALSE
  # Print for status check
  print(paste0("Getting data for event ", i, " of ", length(event_links)))
  # Try and get results
  tempResults <- tryCatch({
      getEventResults(event_links[i])
    }, 
    error=function(cond) {
      skip_ <<- TRUE
    }
  )
  
  # Check if need to skip
  if(skip_) { next }
  # If not skipping
  else {
    # Check if results exists
    if(exists("results") & is.data.frame(tempResults)) {
      results <- plyr::rbind.fill(results, tempResults)
    } else if(is.data.frame(tempResults)) {
      results <- tempResults
    } else {
      print(paste0("Error getting results for ", eventLinks[i]))
      next 
    } 
  }

}

# Add load date
results <- results %>%
  mutate(
    load_date = lubridate::today()
  )

# Create database for raw data and write
# dbCreateTable(pg, "meet_results_raw", results)
dbWriteTable(pg, "meet_results_raw", results, append = TRUE)
