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
library(zoo)
library(dplyr)
library(lubridate)
library(fastverse)
library(tidytable)
library(data.table)

# Source file for functions
source("/Users/samivanecky/git/runneR//scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/altConversinFxns.R")

# Read in conversion data for track size
trckSz <- read.csv("/Users/samivanecky/git/runneR/ncaa_size_conversion.csv")
trckSz <- trckSz %>%
  mutate(
    gender = case_when(
      gender == "men" ~ "M",
      T ~ "F"
    )
  )

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

# Status update
print("Loading base data...")

# Query data tables from AWS
lines <- dbGetQuery(pg, "select * from runner_line_item_raw") %>%
  select(-c(meet_facility, meet_date, meet_track_size))

# # Get last load date from details table
# last_load <- dbGetQuery(pg, "select max(load_d) as last_loaded from runner_lines_details")
# 
# # Subset data
# lines_sub <- lines %>%
#   filter(load_d > last_load$last_loaded)

# Query meet dates
meet_dates <- dbGetQuery(pg, "select * from meet_dates") %>%
  mutate(
    year = as.character(lubridate::year(lubridate::ymd(meet_date))),
    meet_date = lubridate::ymd(meet_date),
    meet_name = trimws(meet_name)
  ) %>%
  # filter(year >= 2016) %>%
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

# Split out data
fieldLines <- lines %>%
  filter.(IS_FIELD == TRUE)

runLines <- lines %>%
  filter.(IS_FIELD == FALSE)

# Remove lines object to clear up memory
rm(lines)

# Convert marks
fieldLines$MARK_TIME <- sapply(fieldLines$MARK_TIME, handleFieldEvents)

# Extract elevation from track information
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
      grepl("western state", meet_facility, ignore.case = T) ~ "200m",
      grepl("utah valley", meet_facility, ignore.case = T) ~ "400m",
      grepl("montana - missoula", meet_facility, ignore.case = T) ~ "400m",
      grepl("colorado st", meet_facility, ignore.case = T) ~ "400m",
      grepl("northern colorado", meet_facility, ignore.case = T) ~ "400m",
      grepl("utep", meet_facility, ignore.case = T) ~ "400m",
      grepl("eastern new mexico", meet_facility, ignore.case = T) ~ "400m",
      grepl("west texas a&m", meet_facility, ignore.case = T) ~ "400m",
      grepl("csu-pueblo", meet_facility, ignore.case = T) ~ "400m",
      grepl("laramie, wy", meet_facility, ignore.case = T) ~ "160m",
      T ~ "400m"
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
      grepl("western state", meet_facility, ignore.case = T) ~ "flat",
      grepl("utah valley", meet_facility, ignore.case = T) ~ "flat",
      grepl("montana - missoula", meet_facility, ignore.case = T) ~ "flat",
      grepl("colorado st", meet_facility, ignore.case = T) ~ "flat",
      grepl("northern colorado", meet_facility, ignore.case = T) ~ "flat",
      grepl("utep", meet_facility, ignore.case = T) ~ "flat",
      grepl("eastern new mexico", meet_facility, ignore.case = T) ~ "flat",
      grepl("west texas a&m", meet_facility, ignore.case = T) ~ "flat",
      grepl("csu-pueblo", meet_facility, ignore.case = T) ~ "flat",
      grepl("laramie, wy", meet_facility, ignore.case = T) ~ "flat",
      T ~ "flat"
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
    track_length = substr(meet_track_size, 1, 5),
    elevation = 0
  )

# Combine data
facs <- rbind(altitude_facilities, facility_sizes) %>%
  mutate(
    # Manually adjust some elevations
    elevation = case_when(
      grepl("alamosa", meet_facility, ignore.case = TRUE) ~ "7545",
      grepl("air force", meet_facility, ignore.case = TRUE) ~ "6981",
      grepl("appalachian state", meet_facility, ignore.case = TRUE) ~ "3333",
      grepl("black hills st", meet_facility, ignore.case = TRUE) ~ "3593",
      grepl("byu", meet_facility, ignore.case = TRUE) ~ "7545",
      grepl("golden, co", meet_facility, ignore.case = TRUE) ~ "5675",
      grepl("boulder, co", meet_facility, ignore.case = TRUE) ~ "5260",
      grepl("pocatello, id", meet_facility, ignore.case = TRUE) ~ "4465",
      grepl("bozeman, mt", meet_facility, ignore.case = TRUE) ~ "4926",
      grepl("texas tech", meet_facility, ignore.case = TRUE) ~ "3281",
      grepl("logan, ut", meet_facility, ignore.case = TRUE) ~ "4680",
      grepl("ogden, ut", meet_facility, ignore.case = TRUE) ~ "4759",
      grepl("laramie, wy", meet_facility, ignore.case = TRUE) ~ "7163",
      T ~ elevation
    )
  ) %>%
  select(-c(meet_track_size)) %>%
  mutate(
    elevation = trimws(elevation),
    track_length = trimws(track_length),
    banked_or_flat = trimws(banked_or_flat)
  )

# Conversion metrics for banked/oversize tracks
# Join facility info
runLines <- runLines %>%
  mutate.(
    MARK_TIME = as.numeric(MARK_TIME)
  ) %>%
  left_join.(
    facs, by = c("meet_facility")
  )

# Clean up facility fields
runLines$elevation = ifelse(is.na(runLines$elevation), 0, runLines$elevation)
runLines$track_length = ifelse(is.na(runLines$track_length), "400m", runLines$track_length)
runLines$banked_or_flat = ifelse(is.na(runLines$banked_or_flat), "flat", runLines$banked_or_flat)

# Create a field for joining track conversions on
runLines <- runLines %>%
  mutate.(
    track_length = as.numeric(gsub("m", "", track_length))
  ) %>%
  mutate.(
    track_size_conversion = case_when(
      track_length < 200 & EVENT %in% c("800m", "Mile", "1500m", "3000m", "5000m", "400m", "DMR", "1000m", "600m", "500m") ~ "undersized",
      track_length == 200 & banked_or_flat == "flat" & EVENT %in% c("800m", "Mile", "1500m", "3000m", "5000m", "400m", "DMR", "1000m", "600m", "500m", "200m", "300m") ~ "flat",
      T ~ "none"
    )
  ) %>%
  mutate.(
    elevation = as.numeric(elevation)
  )

# Status update
print("Performing altitude conversions...")

# Generate altitude converted mark
runLines$altConvMark = mapply(getConv, alt = runLines$elevation, event = runLines$EVENT, mark = runLines$MARK_TIME)

# Generate track size converted mark
# Join track size fields for conversion
runLines <- runLines %>%
  left_join.(
    trckSz, by = c("EVENT" = "event", "GENDER" = "gender", "track_size_conversion" = "type")
  )

# Create final converted mark (alt + track)
runLines$conversion = ifelse(is.na(runLines$conversion), 1, runLines$conversion)
runLines <- runLines %>%
  mutate.(
    converted_time = conversion * altConvMark
  )


# Create calendar data
cal <- genCal()

# Status update
print("Running rolling mark updates...")

# Rewriting code but for tidytable
linesRunner <- runLines %>%
  arrange.(NAME, GENDER, TEAM, DIVISION, EVENT, meet_date) %>%
  mutate.(
    MARK_SHIFT = lag(converted_time) - converted_time,
    PLACE_SHIFT = lag(PLACE) - PLACE,
    DAYS_BTWN = meet_date - lag(meet_date),
    C.PR = cummin(converted_time), # Cumulative PR
    R.MARK = zoo::rollmean(converted_time, k = 2, fill = NA, align = "right"), 
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
lines <- plyr::rbind.fill(linesField, linesRunner)

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

# Status update
print("Calculating PR shift data...")

# Calculate PR data
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
  left_join(cal, by = c("meet_date" = "cal_d")) %>%
  mutate(
    load_d = lubridate::today()
  )

# Status update
print("Uploading to table...")

# Write to table
# dbCreateTable(pg, "runner_lines_details", rl)
dbWriteTable(pg, "runner_lines_details", rl, overwrite = TRUE)
