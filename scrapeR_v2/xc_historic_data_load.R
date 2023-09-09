# Code to generate line item performances from TFRRS for XC meets
library(tidymodels)
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
library(fastverse)
library(tidytable)
library(data.table)
library(yaml)
library(arrow)
library(foreach)
library(doParallel)

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

# Read meet name links
meet_links <- getMeetLinks()

# Remove dups
meet_links <- funique(meet_links)

# Get already scraped links
scraped_links <- dbGetQuery(pg, "select * from meet_links")

# Combine the two
meet_links <- rbind(meet_links, scraped_links) %>%
  funique()

# Remove double backslash
meet_links$link <- gsub("org//", "org/", meet_links$link)

# Write these links to the meet_links database so we don't scrape in the future
# dbRemoveTable(pg, "meet_links")
# dbWriteTable(pg, "meet_links", meet_links, overwrite = TRUE)

# Convert xc_links to vector 
xc_links <- meet_links$link

# Subset XC links
xc_links <- xc_links[grepl("xc", xc_links, ignore.case = T)]

# Error URLs
err_urls <- c()

# Convert xc_links to vector 
xc_links <- xc_links$link

# TEST
#for (i in 1:length(xc_links)) {
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

# Attempt to parallelize the loop
# Function for running in parallel
get_xc_results_in_par <- function(url) {
  # Print status
  print(paste0("Getting data for: ", url))
  # Try and get data
  temp_res <- tryCatch({
    getXCResults(url)
  }, 
  error=function(cond) {
    message("Here's the original error message:")
    message(cond)
    err_urls <- append(err_urls, url)
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
  # Combine output into a list
  return(list(team=team_tbl, ind=ind_tbl))
}

# Detemine CPU cores and use all but one
num_cores <- detectCores() - 1 

# Initialize a parallel backend
cl <- makeCluster(num_cores)  # Use 4 CPU cores
registerDoParallel(cl)

# Run function in parallel and collect the results
xc_results <- foreach(i = 1:length(xc_links), .combine = rbind) %dopar% {
  # Source file for getXCResults
  source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
  # Run function to get results
  tryCatch({
      # Return value
      return(get_xc_results_in_par(xc_links[i]))
    },  
    error=function(cond) {
      print(paste0("Error for link URL: ", xc_links[i]))
    }
  )
}

# Stop cluster
stopCluster(cl)

# Convert matrix to dataframe
xc_results_df <- as.data.frame(xc_results)

# Remove matrix from memory
rm(xc_results)

# Create base tables
# Team
team_tbl <- as.data.frame(xc_results_df$team[1])

# Remove characters in column names before "teams"
colnames(team_tbl) <- sub(".*teams\\.", "", colnames(team_tbl))

# Individual
ind_tbl <- as.data.frame(xc_results_df$ind[1])

# Remove characters in column names before "teams"
colnames(ind_tbl) <- sub(".*individuals\\.", "", colnames(ind_tbl))

# Extract and bind the dataframes from the results
for (i in 2:nrow(xc_results_df)) {
  # Status check
  print(paste0("Getting data for row ", i))
  
  tryCatch({
    # Team
    tmp_team_tbl <- as.data.frame(xc_results_df$team[i])
    
    # Remove characters in column names before "teams"
    colnames(tmp_team_tbl) <- sub(".*teams\\.", "", colnames(tmp_team_tbl))
    
    # Individual
    tmp_ind_tbl <- as.data.frame(xc_results_df$ind[i])
    
    # Remove characters in column names before "teams"
    colnames(tmp_ind_tbl) <- sub(".*individuals\\.", "", colnames(tmp_ind_tbl))
    
    # Bind to final tables
    ind_tbl <- rbind(ind_tbl, tmp_ind_tbl)
    team_tbl <- rbind(team_tbl, tmp_team_tbl)
  },  
  error=function(cond) {
    print(paste0("Error"))
  }
  )
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

# Write to tables
dbRemoveTable(pg, "xc_team_raw")
dbRemoveTable(pg, "xc_ind_raw")
dbCreateTable(pg, "xc_team_raw", team_tbl)
dbWriteTable(pg, "xc_team_raw", team_tbl, append = TRUE)
dbCreateTable(pg, "xc_ind_raw", ind_tbl)
dbWriteTable(pg, "xc_ind_raw", ind_tbl, append = TRUE)

