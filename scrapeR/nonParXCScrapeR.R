# Code to generate line item performances from TFRRS in non-parallel process.
# Code uses the XC meet scraping function to get runner links. Identical code for
# TF can be found in the nonParTFScrapeR.R file
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

# Create a links DF to upload links to AWS table, storing links for meets that have been scraped
linksDf <- as.data.frame(links)

# Query links from link table
linkTbl <- dbGetQuery(aws, "select * from meet_links")

# Get new links (not in table)
joinLinks <- linksDf %>%
  filter(!(links %in% linkTbl$links))

# Write data to table for URLs
# dbRemoveTable(aws, "meet_links")
# dbCreateTable(aws, "meet_links", linksDf)
dbWriteTable(aws, "meet_links", joinLinks, append = TRUE)

# Convert back to vector
joinLinks <- joinLinks$links

# Vector to hold runner URLs
runnerLinks <- vector()

# links <- c("https://www.tfrrs.org/results/xc/19300/NCAA_DI_Cross_Country_Championships", "https://www.tfrrs.org/results/xc/17712/NCAA_DI_Cross_Country_Championships",
#            "https://www.tfrrs.org/results/xc/16731/NCAA_Division_I_Cross_Country_Championships", "https://www.tfrrs.org/results/xc/13423/NCAA_Division_I_Cross_Country_Championships",
#            "https://www.tfrrs.org/results/xc/15036/NCAA_DI_Cross_Country_Championships", "https://www.tfrrs.org/results/xc/11271/NCAA_Division_I_Cross_Country_Championships",
#            "https://www.tfrrs.org/results/xc/9347/NCAA_Division_I_Cross_Country_Championships", "https://www.tfrrs.org/results/xc/6218/NCAA_Division_I_Cross_Country_Championships",
#            "https://www.tfrrs.org/results/xc/6218/NCAA_Division_I_Cross_Country_Championships")

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
for (i in 1:(length(joinLinks))) {
  # Check url
  # tempURL <- gsub("[[:space:]]", "", links[i])
  tempURL <- joinLinks[i]
  
  # Check URL validity
  if(class(try(tempURL %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html())) == 'try-error') {
    print(paste0("Failed to get data for : ", tempURL))
    next
  }
  
  # Print message for meet
  print(paste0("Getting data for: ", tempURL))
 
  # Get runner URLs
  tempLinks <- getRunnerURLs(links[i])
  
  # Bind runners 
  runnerLinks <- append(runnerLinks, tempLinks)

}

# Get unqiue runners
runnerLinks <- funique(runnerLinks)

# Get runner data
# Error links
errorLinks <- vector()

for (i in 1:length(runnerLinks)) {
  
  paste0("Getting data for runner ", i, " out of ", length(runnerLinks))

    tryCatch({
      # Get runner
      tempRunner <- runnerScrape(runnerLinks[i])
      # Bind to temp df
      runner_lines <- rbind(runner_lines, tempRunner)
      },  
      error=function(cond) {
        message("Here's the original error message:")
        message(cond)
        # Choose a return value in case of error
        errorLinks <- append(errorLinks, runnerLinks[i])
        # Sys.sleep(60)
      }
    )
  
}

# Upload to AWS database
# Pull current data out of table
currentData <- dbGetQuery(aws, "select * from race_results") %>%
  select(-c(load_d))

# Add load date to all records being uploaded
runRecs <- runner_lines %>%
  filter(MEET_NAME != "meet") %>%
  funique() %>%
  mutate(
    TIME = as.numeric(TIME),
    PLACE = as.numeric(PLACE),
    MEET_DATE = lubridate::ymd(MEET_DATE),
    NAME = gsub("[^\x01-\x7F]", "", NAME)
  ) %>%
  mutate(
    RUNNER_KEY = paste0(NAME, "-", GENDER, "-", TEAM)
  )
  
# Join data from old & new
uploadData <- rbind(runRecs, currentData) %>%
  funique() %>%
  mutate(
    load_d = lubridate::today()
  )

# Upload runner data to table
# Write data to table for URLs
# dbRemoveTable(aws, "race_results")
# dbCreateTable(aws, "race_results", runRecs)
dbWriteTable(aws, "race_results", runRecs, overwrite = TRUE)

# Update grouped tables
# Group data
runnerGrp <- groupedResults(uploadData)
runnerGrpYrly <- groupedYearlyResults(uploadData)

dbRemoveTable(aws, "results_grouped")
dbRemoveTable(aws, "results_grouped_yr")
dbCreateTable(aws, "results_grouped", runnerGrp)
dbCreateTable(aws, "results_grouped_yr", runnerGrpYrly)
dbWriteTable(aws, "results_grouped", runnerGrp, overwrite = TRUE)
dbWriteTable(aws, "results_grouped_yr", runnerGrpYrly, overwrite = TRUE)

