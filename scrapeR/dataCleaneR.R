# DATA CLEANER
# Script to clean up running data and upload into a database table(s)

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
library(fuzzyjoin)
library(zoo)
library(dplyr)
library(lubridate)
library(fastverse)
library(tidytable)
library(data.table)

# Connect to AWS
# Read connection data from yaml
aws.yml <- read_yaml("/Users/samivanecky/git/TrackPowerRankings/aws.yaml")

# Source file for functions
source("/Users/samivanecky/git/runneR//scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")

# Connect to database
aws <- dbConnect(
  RPostgres::Postgres(),
  host = aws.yml$host,
  user = aws.yml$user,
  password = aws.yml$password,
  port = aws.yml$port
)

# Connect to Postgres
pg <- dbConnect(
  RPostgres::Postgres(),
  host = 'localhost',
  user = 'samivanecky',
  password = 'trackrabbit',
  port = 5432
)

# Function to return times in human readble format
reformatTimes <- function(mark) {
  sec <- str_pad((mark %% 60), width = 2, side = "left", pad = "0")
  min <- floor(mark / 60)
  time <- paste0(min, ":", sec)
  return(time)
}

handleFieldEvents <- function(mark) {
  # Check for meters
  if (grepl("m", mark)) {
    newMark <- as.numeric(gsub("m", "", mark))
    return(newMark)
  } else if (grepl("'", mark)) { # Check for feet
    newMark <- as.numeric(gsub("'", "", mark)) * 0.3048
    return(newMark)
  } else { # Handle all other situations
    return(-999)
  }
}

convertMarks <- function(is_field, mark) {
  MARK_TIME = case_when(
    is_field == TRUE ~ handleFieldEvents(mark),
    T ~ as.numeric(mark)
  )
  
  return(mark)
}

# Query data tables from AWS
lines <- dbGetQuery(aws, "select * from race_results")

# Query meet dates
meet_dates <- dbGetQuery(aws, "select * from meet_dates") %>%
  mutate(
    year = as.character(lubridate::year(lubridate::ymd(meet_date))),
    meet_date = lubridate::ymd(meet_date),
    meet_name = trimws(meet_name)
  ) %>%
  filter(year >= 2016) %>%
  funique() %>%
  mutate(
    meet_name = tolower(meet_name)
  )

# Attempt to join
lines <- lines %>%
  mutate(
    MEET_NAME = tolower(MEET_NAME)
  ) %>%
  left_join(meet_dates, by = c("MEET_NAME" = "meet_name", "YEAR" = "year")) %>%
  funique() %>%
  select(-c(MEET_DATE, load_d)) %>%
  rowwise() %>%
  mutate(
    MARK_TIME = handleTimes(MARK)
  )

# Convert to a data table
lines <- as.data.table(lines)

fieldLines <- lines %>%
  filter.(IS_FIELD == TRUE)

runLines <- lines %>%
  filter.(IS_FIELD == FALSE)

# Convert marks
fieldLines$MARK_TIME <- sapply(fieldLines$MARK_TIME, handleFieldEvents)

# Clean up meet data
# Subset altitude locations
altitude_facilities <- meet_dates %>%
  filter(meet_track_size != "no location" & !grepl("timing", meet_track_size, ignore.case = TRUE)) %>%
  filter(grepl("elevation", meet_track_size, ignore.case = TRUE)) %>%
  select(meet_facility, meet_track_size) %>%
  funique() %>%
  mutate(
    elevation = substr(meet_track_size, 1, 5)
  ) %>%
  mutate(
    track_length = case_when(
      grepl("byu", meet_facility, ignore.case = T) ~ "300m",
      grepl("boulder", meet_facility, ignore.case = T) ~ "300m",
      grepl("unm", meet_facility, ignore.case = T) ~ "200m",
      grepl("boulder", meet_facility, ignore.case = T) ~ "300m",
      grepl("black hills st", meet_facility, ignore.case = T) ~ "200m",
      grepl("air force", meet_facility, ignore.case = T) ~ "268m",
      grepl("appalachian", meet_facility, ignore.case = T) ~ "300m",
      grepl("montana state", meet_facility, ignore.case = T) ~ "200m",
      grepl("colorado mines", meet_facility, ignore.case = T) ~ "193m",
      grepl("utah state", meet_facility, ignore.case = T) ~ "200m",
      grepl("weber state", meet_facility, ignore.case = T) ~ "200m",
      grepl("idaho state", meet_facility, ignore.case = T) ~ "200m",
      grepl("texas tech", meet_facility, ignore.case = T) ~ "200m",
      
      T ~ "uncat"
    )
  ) %>%
  mutate(
    banked_or_flat = case_when(
      track_length == "300m" ~ "flat",
      grepl("unm", meet_facility, ignore.case = T) ~ "banked",
      grepl("black hills st", meet_facility, ignore.case = T) ~ "flat",
      grepl("air force", meet_facility, ignore.case = T) ~ "flat",
      grepl("montana state", meet_facility, ignore.case = T) ~ "flat",
      grepl("colorado mines", meet_facility, ignore.case = T) ~ "flat",
      grepl("utah state", meet_facility, ignore.case = T) ~ "banked",
      grepl("weber state", meet_facility, ignore.case = T) ~ "flat",
      grepl("idaho state", meet_facility, ignore.case = T) ~ "banked",
      grepl("texas tech", meet_facility, ignore.case = T) ~ "banked",
      
      T ~ "uncat"
    )
  )

# Subset track sizes
facility_sizes <- meet_dates %>%
  filter(meet_track_size != "no location" & !grepl("timing", meet_track_size, ignore.case = TRUE)) %>%
  filter(!grepl("elevation", meet_track_size, ignore.case = TRUE)) %>%
  select(meet_facility, meet_track_size) %>%
  funique() %>%
  mutate(
    banked_or_flat = case_when(
      grepl("bank", meet_track_size, ignore.case = TRUE) ~ "banked",
      T ~ "flat"
    ),
    track_length = substr(meet_track_size, 1, 5)
  )



runLines <- runLines %>%
  mutate.(
    MARK_TIME = as.numeric(MARK_TIME)
  )

# Create calendar data
cal <- genCal()

# Rewriting code but for tidytable
linesRunner <- runLines %>%
  arrange.(NAME, GENDER, TEAM, DIVISION, EVENT, meet_date) %>%
  mutate.(
    MARK_SHIFT = lag(MARK_TIME) - MARK_TIME,
    PLACE_SHIFT = lag(PLACE) - PLACE,
    DAYS_BTWN = meet_date - lag(meet_date),
    C.PR = cummin(MARK_TIME), # Cumulative PR
    R.MARK = zoo::rollmean(MARK_TIME, k = 2, fill = NA, align = "right"), 
    R.PLACE = zoo::rollmean(PLACE, k = 2, fill = NA, align = "right"),
    .by = c(NAME, GENDER, TEAM, DIVISION, EVENT)
  ) %>%
  mutate.(
    R.PLACE_SHIFT = zoo::rollmean(PLACE_SHIFT, k = 2, fill = NA, align = "right"),
    R.MARK_SHIFT = zoo::rollmean(MARK_SHIFT, k = 2, fill = NA, align = "right"),
    EVENT_TYPE = case_when(
      grepl("XC", EVENT, ignore.case = TRUE) | (lubridate::month(meet_date) > 6 & lubridate::month(meet_date) < 12) ~ "XC",
      (lubridate::month(meet_date) >= 1 & lubridate::month(meet_date) <= 2) | (lubridate::month(meet_date) == 3 & lubridate::day(meet_date) < 20) |
        lubridate::month(meet_date) == 12 ~ "INDOOR",
      (lubridate::month(meet_date) >= 4 & lubridate::month(meet_date) <= 6) | (lubridate::month(meet_date) == 3 & lubridate::day(meet_date) >= 20) ~ "OUTDOOR",
      T ~ "OTHER"
    ),
    .by = c(NAME, GENDER, TEAM, DIVISION, EVENT)
  )

# For field athletes
linesField <- fieldLines %>%
  arrange.(NAME, GENDER, TEAM, DIVISION, EVENT, meet_date) %>%
  mutate.(
    MARK_SHIFT = lag(MARK_TIME) - MARK_TIME,
    PLACE_SHIFT = lag(PLACE) - PLACE,
    DAYS_BTWN = meet_date - lag(meet_date),
    C.PR = cummax(MARK_TIME), # Cumulative PR
    R.MARK = zoo::rollmean(MARK_TIME, k = 2, fill = NA, align = "right"), 
    R.PLACE = zoo::rollmean(PLACE, k = 2, fill = NA, align = "right"),
    .by = c(NAME, GENDER, TEAM, DIVISION, EVENT)
  ) %>%
  mutate.(
    R.PLACE_SHIFT = zoo::rollmean(PLACE_SHIFT, k = 2, fill = NA, align = "right"),
    R.MARK_SHIFT = zoo::rollmean(MARK_SHIFT, k = 2, fill = NA, align = "right"),
    EVENT_TYPE = case_when(
      grepl("XC", EVENT, ignore.case = TRUE) | (lubridate::month(meet_date) > 6 & lubridate::month(meet_date) < 12) ~ "XC",
      (lubridate::month(meet_date) >= 1 & lubridate::month(meet_date) <= 2) | (lubridate::month(meet_date) == 3 & lubridate::day(meet_date) < 20) |
        lubridate::month(meet_date) == 12 ~ "INDOOR",
      (lubridate::month(meet_date) >= 4 & lubridate::month(meet_date) <= 6) | (lubridate::month(meet_date) == 3 & lubridate::day(meet_date) >= 20) ~ "OUTDOOR",
      T ~ "OTHER"
    ),
    .by = c(NAME, GENDER, TEAM, DIVISION, EVENT)
  )

# Bind data together
lines <- rbind(linesField, linesRunner)

# Filter out bad data
rl <- lines %>%
  filter.(
    (GENDER == "F" & !((EVENT == "800m" & (MARK_TIME <= 115 | MARK_TIME >= 260)) | (EVENT == "Mile" & (MARK_TIME <= 240 | MARK_TIME >= 540)) |
                         (EVENT == "1500m" & (MARK_TIME <= 230 | MARK_TIME >= 530)) | (EVENT == "3000m" & (MARK_TIME <= 490 | MARK_TIME >= 900)) |
                         (EVENT == "5000m" & (MARK_TIME <= 900 | MARK_TIME >= 1500)))) |
      (GENDER == "M" & !((EVENT == "800m" & (MARK_TIME <= 100 | MARK_TIME >= 240)) | (EVENT == "Mile" & (MARK_TIME <= 200 | MARK_TIME >= 500)) |
                           (EVENT == "1500m" & (MARK_TIME <= 190 | MARK_TIME >= 490)) | (EVENT == "3000m" & (MARK_TIME <= 400 | MARK_TIME >= 900)) |
                           (EVENT == "5000m" & (MARK_TIME <= 660 | MARK_TIME >= 1500))))
  ) %>%
  filter.(
      !(MEET_NAME == "WSC Relays and Multi-Events Qualifier" & EVENT == "800m") &
      !(EVENT == "10K" & EVENT_TYPE == "INDOOR") &
      !(MARK_TIME < 120 & GENDER == "F" & EVENT == "800m" & !(NAME == "ATHING MU" | NAME == "RAEVYN ROGERS")) &
      !(EVENT == "800m" & MARK_TIME < 102) &
      !(grepl("Olympic Trials", MEET_NAME, ignore.case = TRUE) | grepl("IAAF World Championships", MEET_NAME, ignore.case = TRUE)) &
      !(EVENT == "10K" & MARK_TIME < 1500)
  )

# Create variable for flagging PR, post pr time change, and a clean time for display
# Additionally, adjusting year for grouping data for YOY purposes
rl <- setDF(rl)

rl <- rl %>%
  mutate(
    is_pr = case_when(
      C.PR == MARK ~ 1,
      T ~ 0
    )
  ) %>%
  group_by(NAME, GENDER, TEAM, DIVISION, EVENT) %>%
  mutate(
    post_pr_shift = case_when(
      is_pr == 1 ~ lead(MARK_SHIFT),
      T ~ -999
    )
  ) %>%
  ungroup() %>%
  mutate(
    competition_year = case_when(
      lubridate::month(meet_date) == 12 & EVENT_TYPE == "INDOOR" ~ lubridate::year(meet_date) + 1,
      T ~ lubridate::year(meet_date)
    )
  )

# Join calendar info
rl <- rl %>%
  left_join.(cal, by = c("meet_date" = "cal_d"))
