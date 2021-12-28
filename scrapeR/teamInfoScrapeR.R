# Code to generate line item performances from TFRRS.
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

# Set system variables
# Sys.setenv("http_proxy"="")
# Sys.setenv("no_proxy"=TRUE)
# Sys.setenv("no_proxy"=1)

# Load functions for scraping
source("/Users/samivanecky/git/TrackPowerRankings/Scraping_Fxns.R")
source("/Users/samivanecky/git/TrackPowerRankings/ResultsQuery.R")
source("/Users/samivanecky/git/TrackPowerRankings/meetScrapingFxns.R")

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

# Division 1
url <- "https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_institutions"

# Read table from link
div1 <- url %>%
  read_html() %>%
  html_table()

# Convert to DFs
# Main schools
tbl1 <- as.data.frame(div1[1])
# Transition schools
tbl2 <- as.data.frame(div1[2])

# Subset columns
tbl1 <- tbl1 %>%
  select(School, Team, City, State, Primary.conference) %>%
  rename(
    Conference = Primary.conference
  )

tbl2 <- tbl2 %>%
  select(School, Team, City, State, Conference)

# Bind tables
d1 <- rbind(tbl1, tbl2)

# Add division
d1 <- d1 %>%
  mutate(
    Division = "D1"
  )

# Division 2
url <- "https://en.wikipedia.org/wiki/List_of_NCAA_Division_II_institutions"

# Read table from link
div2 <- url %>%
  read_html() %>%
  html_table()

# Convert to DFs
# Main schools
tbl1 <- as.data.frame(div2[1])
# Transition schools
tbl2 <- as.data.frame(div2[2])

# Subset columns
tbl1 <- tbl1 %>%
  select(School, Nickname, City, State.Province, Conference) %>%
  rename(
    State = State.Province,
    Team = Nickname
  )

tbl2 <- tbl2 %>%
  select(School, Nickname, City, State.Province, Conference) %>%
  rename(
    State = State.Province,
    Team = Nickname
  )

# Bind tables
d2 <- rbind(tbl1, tbl2)

d2 <- d2 %>%
  mutate(
    Division = "D2"
  )


# Division 3
url <- "https://en.wikipedia.org/wiki/List_of_NCAA_Division_III_institutions"

# Read table from link
div3 <- url %>%
  read_html() %>%
  html_table()

# Convert to DFs
# Main schools
tbl1 <- as.data.frame(div3[1])

# Subset columns
tbl1 <- tbl1 %>%
  select(School, Nickname, City, State, Conference) %>%
  rename(
    Team = Nickname
  )


# Bind tables
d3 <- tbl1

d3 <- d3 %>%
  mutate(
    Division = "D3"
  )


# Bind all tables
divs <- rbind(d1, d2, d3)

# Write data to table for URLs
# dbRemoveTable(aws, "team_info")
dbCreateTable(aws, "team_info", divs)
dbWriteTable(aws, "team_info", divs, overwrite = TRUE)

