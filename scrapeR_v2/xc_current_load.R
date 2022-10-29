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
meet_links <- getCurrentMeetLinks()

# Remove dups
meet_links <- funique(meet_links)

# Convert to df
meet_links <- as.data.frame(meet_links)
names(meet_links) <- c("link")

# Get already scraped links
scraped_links <- dbGetQuery(pg, "select * from meet_links")

# Filter out already scraped links
meet_links <- meet_links %>%
  filter(
    !(link %in% scraped_links$link)
  )

# Convert to data frame
meet_links <- as.data.frame(meet_links)

# Write new links to table
dbWriteTable(pg, "meet_links", meet_links, append = TRUE)

# Convert back to vector
meet_links <- meet_links$link

# Subset XC links
xc_links <- meet_links[grepl("xc", meet_links, ignore.case = T)]

# Error URLs
err_urls <- c()

# Scrape results
for (i in 1:length(xc_links)) {
  # Print status
  print(paste0("Getting data for: ", xc_links[i], " which is ", i, " of ", length(xc_links)))
  # Try and get data
  temp_res <- tryCatch({
    getXCResults(xc_links[i])
  }, 
  error=function(cond) {
    message("Here's the original error message:")
    message(cond)
    err_urls <- append(err_urls, xc_links[i])
    # Sys.sleep(60)
    return(NA)
  })
  # Check for error
  if(!any(is.na(temp_res))) {
    # Append data
    if(exists("team_tbl")) {
      team_tbl <- rbind(team_tbl, as.data.frame(temp_res["teams"]))
      ind_tbl <- rbind(ind_tbl, as.data.frame(temp_res["individuals"]))
    } else {
      team_tbl <- as.data.frame(temp_res["teams"])
      ind_tbl <- as.data.frame(temp_res["individuals"])
    }
  }
}

# Add load dates to data
team_tbl <- team_tbl %>%
  mutate(
    load_d = lubridate::today()
  )

ind_tbl <- ind_tbl %>%
  mutate(
    load_d = lubridate::today()
  )

# Convert to dataframes
team_tbl <- as.data.frame(team_tbl)
ind_tbl <- as.data.frame(ind_tbl)

# Write to tables
dbWriteTable(pg, "xc_team_raw", team_tbl, append = TRUE)
dbWriteTable(pg, "xc_ind_raw", ind_tbl, append = TRUE)

