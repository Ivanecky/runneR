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
library(ggthemes)
library(ggridges)
library(ggrepel)
library(zoo)
library(dplyr)
library(teamcolors)
library(gt)
library(lubridate)

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

# Function to return times in human readble format
reformatTimes <- function(mark) {
  sec <- str_pad((mark %% 60), width = 2, side = "left", pad = "0")
  min <- floor(mark / 60)
  time <- paste0(min, ":", sec)
  return(time)
}

# Query data tables from AWS
lines <- dbGetQuery(aws, "select * from race_results")

# Create calendar data
cal <- genCal()

lines <- lines %>%
  group_by(NAME, GENDER, TEAM, DIVISION, EVENT) %>%
  arrange(MEET_DATE, .by_group = TRUE) %>%
  mutate(
    MARK_SHIFT = lag(MARK) - MARK,
    PLACE_SHIFT = lag(PLACE) - PLACE,
    DAYS_BTWN = MEET_DATE - lag(MEET_DATE),
    C.PR = cummin(MARK), # Cumulative PR
    R.MARK = zoo::rollmean(MARK, k = 2, fill = NA, align = "right"), # Rolling time (past 2 marks)
    R.PLACE = zoo::rollmean(PLACE, k = 2, fill = NA, align = "right") # Rolling place (past 2 marks)
  ) %>%
  mutate(
    R.PLACE_SHIFT = zoo::rollmean(PLACE_SHIFT, k = 2, fill = NA, align = "right"),
    R.MARK_SHIFT = zoo::rollmean(MARK_SHIFT, k = 2, fill = NA, align = "right"),
    EVENT_TYPE = case_when(
      grepl("XC", EVENT, ignore.case = TRUE) | (lubridate::month(MEET_DATE) > 6 & lubridate::month(MEET_DATE) < 12) ~ "XC",
      (lubridate::month(MEET_DATE) >= 1 & lubridate::month(MEET_DATE) <= 2) | (lubridate::month(MEET_DATE) == 3 & lubridate::day(MEET_DATE) < 20) |
        lubridate::month(MEET_DATE) == 12 ~ "INDOOR",
      (lubridate::month(MEET_DATE) >= 4 & lubridate::month(MEET_DATE) <= 6) | (lubridate::month(MEET_DATE) == 3 & lubridate::day(MEET_DATE) >= 20) ~ "OUTDOOR",
      T ~ "OTHER"
    )
  ) %>%
  select(-c(RUNNER_KEY, load_d))

# Create variable for flagging PR, post pr time change, and a clean time for display
# Additionally, adjusting year for grouping data for YOY purposes
lines <- lines %>%
  mutate(
    is_pr = case_when(
      C.PR == MARK ~ 1,
      T ~ 0
    )
  ) %>%
  mutate(
    post_pr_shift = case_when(
      is_pr == 1 ~ lead(MARK_SHIFT),
      T ~ -999
    )
  ) %>%
  rowwise() %>%
  mutate(
    clean_time = reformatTimes(TIME),
    competition_year = case_when(
      lubridate::month(MEET_DATE) == 12 & EVENT_TYPE == "INDOOR" ~ lubridate::year(MEET_DATE) + 1,
      T ~ lubridate::year(MEET_DATE)
    )
  ) %>%
  filter(!is.na(MEET_NAME) & MEET_NAME != "")

# Filter out bad data
rl <- lines %>%
  filter(
    (GENDER == "F" & !((EVENT == "800m" & (TIME <= 115 | TIME >= 260)) | (EVENT == "Mile" & (TIME <= 240 | TIME >= 540)) |
                         (EVENT == "1500m" & (TIME <= 230 | TIME >= 530)) | (EVENT == "3000m" & (TIME <= 490 | TIME >= 900)) |
                         (EVENT == "5000m" & (TIME <= 900 | TIME >= 1500)))) |
      (GENDER == "M" & !((EVENT == "800m" & (TIME <= 100 | TIME >= 240)) | (EVENT == "Mile" & (TIME <= 200 | TIME >= 500)) |
                           (EVENT == "1500m" & (TIME <= 190 | TIME >= 490)) | (EVENT == "3000m" & (TIME <= 400 | TIME >= 900)) |
                           (EVENT == "5000m" & (TIME <= 660 | TIME >= 1500))))
  ) %>%
  filter(
      !(MEET_NAME == "WSC Relays and Multi-Events Qualifier" & EVENT == "800m") &
      !(EVENT == "10K" & EVENT_TYPE == "INDOOR") &
      !(TIME < 120 & GENDER == "F" & EVENT == "800m" & !(NAME == "ATHING MU" | NAME == "RAEVYN ROGERS")) &
      !(EVENT == "800m" & TIME < 102) &
      !(grepl("Olympic Trials", MEET_NAME, ignore.case = TRUE) | grepl("IAAF World Championships", MEET_NAME, ignore.case = TRUE)) &
      !(EVENT == "10K" & TIME < 1500)
  )

rl <- rl %>%
  left_join(cal, by = c("MEET_DATE" = "cal_d"))
