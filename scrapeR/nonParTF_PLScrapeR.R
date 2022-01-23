# RUN THIS FOR INDOOR MEET SCRAPING
# Code to generate line item performances from TFRRS in non-parallel process.
# Code uses the TF meet scraping function to get runner links. Identical code for
# XC can be found in the nonParXCScrapeR.R file
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
aws.yml <- read_yaml("/Users/samivanecky/git/TrackPowerRankings/aws.yaml")

# Connect to database
aws <- dbConnect(
  RPostgres::Postgres(),
  host = aws.yml$host,
  user = aws.yml$user,
  password = aws.yml$password,
  port = aws.yml$port
)

joinLinks <- c("https://www.tfrrs.org/lists/2770/2019_2020_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2020/i",
               "https://www.tfrrs.org/lists/2771/2019_2020_NCAA_Div._II_Indoor_Qualifying_(FINAL)",
               "https://www.tfrrs.org/lists/2772/2019_2020_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2020/i",
               "https://www.tfrrs.org/archived_lists/2568/2019_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2019/o",
               "https://www.tfrrs.org/archived_lists/2571/2019_NCAA_Div._II_Outdoor_Qualifying_(FINAL)/2019/o",
               "https://www.tfrrs.org/archived_lists/2572/2019_NCAA_Div._III_Outdoor_Qualifying_(FINAL)/2019/o",
               "https://www.tfrrs.org/archived_lists/2324/2018_2019_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2019/i",
               "https://www.tfrrs.org/archived_lists/2325/2018_2019_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2019/i",
               "https://www.tfrrs.org/archived_lists/2326/2018_2019_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2019/i",
               "https://www.tfrrs.org/archived_lists/2279/2018_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2018/o",
               "https://www.tfrrs.org/archived_lists/2282/2018_NCAA_Div._II_Outdoor_Qualifying_(FINAL)/2018/o",
               "https://www.tfrrs.org/archived_lists/2283/2018_NCAA_Div._III_Outdoor_Qualifying_(FINAL)/2018/o",
               "https://www.tfrrs.org/archived_lists/2124/2017_2018_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2018/i",
               "https://www.tfrrs.org/archived_lists/2125/2017_2018_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2018/i",
               "https://www.tfrrs.org/archived_lists/2126/2017_2018_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2018/i",
               "https://www.tfrrs.org/archived_lists/1912/2017_NCAA_Div._I_Outdoor_Qualifying_(FINAL)/2017/o",
               "https://www.tfrrs.org/archived_lists/1913/2017_NCAA_Div._II_Outdoor_Qualifying_(FINAL)/2017/o",
               "https://www.tfrrs.org/archived_lists/1914/2017_NCAA_Div._III_Outdoor_Qualifying_(FINAL)/2017/o",
               "https://www.tfrrs.org/archived_lists/1797/2016_2017_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2017/i",
               "https://www.tfrrs.org/archived_lists/1798/2016_2017_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2017/i",
               "https://www.tfrrs.org/archived_lists/1799/2016_2017_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2017/i",
               "https://www.tfrrs.org/lists/3191/2021_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2021/o",
               "https://www.tfrrs.org/lists/3194/2021_NCAA_Division_II_Outdoor_Qualifying_(FINAL)",
               "https://www.tfrrs.org/lists/3195/2021_NCAA_Division_III_Outdoor_Qualifying_(FINAL)/2021/o",
               "https://www.tfrrs.org/lists/3196/2021_NAIA_Outdoor_Qualifying_List_(FINAL)",
               "https://www.tfrrs.org/lists/3157/2020_2021_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2021/i",
               "https://www.tfrrs.org/lists/3158/2020_2021_NCAA_Div._II_Indoor_Qualifying_(FINAL)",
               "https://www.tfrrs.org/lists/3161/2020_2021_NCAA_Division_III_Indoor_Qualifying_List/2021/i",
               "https://www.tfrrs.org/lists/3156/2020_2021_NAIA_Indoor_Qualifying_(FINAL)/2021/i")

meetLinks <- c()

for ( i in 1:length(joinLinks)) {
  # Get meet URLs
  meetUrls <- getPLMeetLinks(joinLinks[i])
  
  # Append
  meetLinks <- append(meetLinks, meetUrls)
}

# Get rid of any doubles
meetLinks <- funique(meetLinks)

# Vector to hold runner URLs
runnerLinks <- vector()

# Create a temporary dataframe for runner line item performance
runner_lines = as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division", FALSE, "1"))
# Rename columns
names(runner_lines) = c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION", "IS_FIELD", "MARK_TIME")
# Reformat var
runner_lines <- runner_lines %>%
  mutate(
    YEAR = as.character(YEAR),
    EVENT = as.character(EVENT),
    PLACE = as.numeric(PLACE),
    NAME = as.character(NAME),
    GENDER = as.character(GENDER),
    TEAM = as.character(TEAM)
  )

# Iterate over meets and get data
for (i in 1:(length(meetLinks))) {
  # Check url
  # tempURL <- gsub("[[:space:]]", "", links[i])
  tempURL <- meetLinks[i]
  
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
  tempLinks <- getIndoorRunnerURLs(meetLinks[i])
  
  # Bind runners 
  runnerLinks <- append(runnerLinks, tempLinks)
  
}

# Get unqiue runners
runnerLinks <- funique(runnerLinks)

# Get runner data
# Error links
errorLinks <- vector()

for (i in 135481:length(runnerLinks)) {
  
  print(paste0("Getting data for runner ", i, " out of ", length(runnerLinks)))
  
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
  mutate(
    RUNNER_KEY = paste0(NAME, "-", GENDER, "-", TEAM)
  ) %>%
  select(-c(load_d)) %>%
  mutate(
    IS_FIELD = FALSE,
    MARK_TIME = TIME
  ) %>%
  rename(
    MARK = TIME
  )

# Add load date to all records being uploaded
runRecs <- runner_lines %>%
  filter(MEET_NAME != "meet") %>%
  funique() %>%
  mutate(
   # TIME = as.numeric(TIME),
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
  ) %>%
  filter(EVENT != "OTHER")

# Upload runner data to table
# Write data to table for URLs
#dbRemoveTable(aws, "race_results")
#dbCreateTable(aws, "race_results", uploadData)
dbWriteTable(aws, "race_results", uploadData, overwrite = TRUE)

# Update grouped tables
# Group data
runnerGrp <- groupedResults(uploadData)
runnerGrpYrly <- groupedYearlyResults(uploadData)

# dbRemoveTable(aws, "results_grouped")
# dbRemoveTable(aws, "results_grouped_yr")
# dbCreateTable(aws, "results_grouped", runnerGrp)
# dbCreateTable(aws, "results_grouped_yr", runnerGrpYrly)
dbWriteTable(aws, "results_grouped", runnerGrp, overwrite = TRUE)
dbWriteTable(aws, "results_grouped_yr", runnerGrpYrly, overwrite = TRUE)