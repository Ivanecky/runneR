# Code to clean XC data
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

# Load functions
source("/Users/samivanecky/git/runneR/scrapeR/Scraping_Fxns.R")

# Connect to PG
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

# Read data
team <- dbGetQuery(pg, "select * from xc_team_raw where load_d in (select max(load_d) from xc_team_raw)")
ind <- dbGetQuery(pg, "select * from xc_ind_raw where load_d in (select max(load_d) from xc_ind_raw)")

#########################
# Clean team data

# Rename cols
team <- team %>%
  rename(
    r1 = X1,
    r2 = X2, 
    r3 = X3,
    r4 = X4,
    r5 = X5,
    r6 = X6,
    r7 = X7,
    Avg_Time = Avg..Time,
    Total_Time = Total.Time
  )

# Create gender and distance fields
team <- team %>%
  mutate(
    gender = case_when(
      grepl("women|woman|girl", RACE_TYPE, ignore.case = TRUE) ~ "F",
      T ~ "M"
    ),
    RACE_TYPE = gsub("Top↑", "", str_squish(RACE_TYPE)),
    race_distance = case_when(
      grepl("5k", RACE_TYPE, ignore.case = TRUE) ~ "5k",
      grepl("6k", RACE_TYPE, ignore.case = TRUE) ~ "6k",
      grepl("4k", RACE_TYPE, ignore.case = TRUE) ~ "4k",
      grepl("8k", RACE_TYPE, ignore.case = TRUE) ~ "8k",
      grepl("10k", RACE_TYPE, ignore.case = TRUE) ~ "10k",
      grepl("3 mile", RACE_TYPE, ignore.case = TRUE) ~ "3 Mile",
      grepl("4 mile", RACE_TYPE, ignore.case = TRUE) ~ "4 Mile",
      grepl("5 mile", RACE_TYPE, ignore.case = TRUE) ~ "5 Mile",
      grepl("2 mile", RACE_TYPE, ignore.case = TRUE) ~ "2 Mile",
      grepl("3.1 mile", RACE_TYPE, ignore.case = TRUE) ~ "5k",
      grepl("4.97 mile", RACE_TYPE, ignore.case = TRUE) ~ "8k",
      grepl("4.98 mile", RACE_TYPE, ignore.case = TRUE) ~ "8k",
      grepl("3k", RACE_TYPE, ignore.case = TRUE) ~ "3k",
      grepl("3.11 mile", RACE_TYPE, ignore.case = TRUE) ~ "5k",
      T ~ "other"
    ),
    race_date = lubridate::ymd(MEET_DT)
  ) %>%
  mutate(
    year = lubridate::year(race_date),
    month = lubridate::month(race_date),
    day = lubridate::day(race_date),
    # Spread figures
    r1_gap = r2 - r1,
    r2_gap = r3 - r2,
    r3_gap = r4 - r3,
    r4_gap = r5 - r4,
    r5_gap = r6 - r5,
    r6_gap = r7 - r6,
    spread = r5 - r1
  ) %>%
  mutate(
    meet_key = paste0(MEET_NAME, " - ", RACE_TYPE, " - ", gender, " - ", race_distance, " - ", year)
  )

# Convert time to numeric
team$avg_time_numeric = sapply(team$Avg_Time, handleTimes)

############################
# Clean individual data

# Fix data
ind <- ind %>%
  mutate(
    NAME = str_squish(NAME),
    TEAM = str_squish(TEAM),
    RACE_TYPE = gsub("Top↑", "", str_squish(RACE_TYPE)),
    PL = as.numeric(PL),
    gender = case_when(
      grepl("women|woman|girl", RACE_TYPE, ignore.case = TRUE) ~ "F",
      T ~ "M"
    ),
    race_distance = case_when(
      grepl("5k", RACE_TYPE, ignore.case = TRUE) ~ "5k",
      grepl("6k", RACE_TYPE, ignore.case = TRUE) ~ "6k",
      grepl("4k", RACE_TYPE, ignore.case = TRUE) ~ "4k",
      grepl("8k", RACE_TYPE, ignore.case = TRUE) ~ "8k",
      grepl("10k", RACE_TYPE, ignore.case = TRUE) ~ "10k",
      grepl("3 mile", RACE_TYPE, ignore.case = TRUE) ~ "3 Mile",
      grepl("4 mile", RACE_TYPE, ignore.case = TRUE) ~ "4 Mile",
      grepl("5 mile", RACE_TYPE, ignore.case = TRUE) ~ "5 Mile",
      grepl("2 mile", RACE_TYPE, ignore.case = TRUE) ~ "2 Mile",
      grepl("3.1 mile", RACE_TYPE, ignore.case = TRUE) ~ "5k",
      grepl("4.97 mile", RACE_TYPE, ignore.case = TRUE) ~ "8k",
      grepl("4.98 mile", RACE_TYPE, ignore.case = TRUE) ~ "8k",
      grepl("3k", RACE_TYPE, ignore.case = TRUE) ~ "3k",
      grepl("3.11 mile", RACE_TYPE, ignore.case = TRUE) ~ "5k",
      T ~ "other"
    ),
    race_date = lubridate::ymd(MEET_DT)
  ) %>%
  mutate(
    year = lubridate::year(race_date),
    month = lubridate::month(race_date),
    day = lubridate::day(race_date),
    # Fix age
    class = case_when(
      grepl("FR|Fresh|Freshman", YEAR, ignore.case = TRUE) ~ "FR",
      grepl("Soph", YEAR, ignore.case = TRUE) | YEAR %in% c("So", "SO", "SO-2", "So.", "SO. C") ~ "SO",
      grepl("Junio|Jr", YEAR, ignore.case = TRUE) ~ "JR",
      grepl("Senior|SR", YEAR, ignore.case = TRUE) ~ "SR",
      T ~ "Other"
    )
  ) %>%
  mutate(
    meet_key = paste0(MEET_NAME, " - ", gender, " - ", race_distance, " - ", year)
  )

# Convert times to numeric
ind$numeric_time <- sapply(ind$TIME, handleTimes)

# Remove any duplicates
ind <- ind %>%
  funique()

# Create time behind winner metric
ind <- ind %>%
  mutate(
    numeric_time = as.numeric(numeric_time)
  ) %>%
  group_by(meet_key) %>%
  dplyr::arrange(numeric_time, .by_group = TRUE) %>%
  mutate(
    time_behind_winner = numeric_time - min(numeric_time, na.rm = TRUE)
  ) %>%
  ungroup()

# Get existing data
ex_teams <- dbGetQuery(pg, "select * from xc_team_dets") %>%
  select(
    -c(load_d)
  )

ex_ind <- dbGetQuery(pg, "select * from xc_ind_dets") %>%
  select(
    -c(load_d)
  )

# Drop load date
team <- team %>%
  select(
    -c(load_d)
  )

ind <- ind %>%
  select(
    -c(load_d)
  )

# Merge old a new
new_upload_ind <- rbind(ex_ind, ind) %>%
  funique()

new_upload_team <- rbind(ex_teams, team) %>%
  funique()

# Write tables
# dbCreateTable(pg, "xc_team_dets", team)
dbWriteTable(pg, "xc_team_dets", new_upload_team, overwrite = TRUE)
# dbCreateTable(pg, "xc_ind_dets", ind)
dbWriteTable(pg, "xc_ind_dets", new_upload_ind, overwrite = TRUE)
