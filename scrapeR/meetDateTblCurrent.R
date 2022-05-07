# CODE DESCRIPTIONG
# This code scrapes performance lists and current meet links to get updated information on facilities.
# Data is used for determining track size, banked/flat, elevation, and location.

# Load libraries
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
library(stringi)

# Load functions for scraping
source("/Users/samivanecky/git/runneR/scrapeR/Scraping_Fxns.R")
# source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/ResultsQuery.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")

# Connect to AWS
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

# Get links that have already been scraped for results
meetLinks <- dbGetQuery(pg, "select * from meet_links")

# Get current meet links
currentLinks <- getMeetLinks()

# Convert to dataframe
currentLinks <- as.data.frame(currentLinks)

newMeetLinks <- currentLinks %>%
  filter(!(currentLinks %in% meetLinks$link))

# Remove dups
newMeetLinks <- funique(newMeetLinks)

# Clean out links that are event specific
meetLinks <- meetLinks[!(grepl("_Jump|_Vault|meteres|Meters|Hurdles|_Throw|Mile|Pentathlon|Heptathlon|Shot_Put|Discus|Hammer|Javelin|Decathlon|Steeplechase", meetLinks, ignore.case = TRUE))]
meetLinks <- meetLinks[!(grepl("_Relay", meetLinks, ignore.case = TRUE) & !grepl("_Relays", meetLinks, ignore.case = TRUE))]

# Vectors to hold info
meetNames <- vector()
meetDates <- vector()
meetFacs <- vector()
meetTrkSz <- vector()
errorLinks <- vector()

# Iterate over meets
for (i in 1:length(newMeetLinks)) {
  # Print out which meet
  print(paste0("Getting data for meet ", newMeetLinks[i]))

  # Go through meets and get dates
  tempUrl <- newMeetLinks[i]

  if(class(try(tempUrl %>%
               GET(., timeout(30), user_agent(randUsrAgnt())) %>%
               read_html())) == 'try-error') {
    print(paste0("Failed to get data for : ", tempUrl))
    errorLinks <- append(errorLinks, tempUrl)
    next
  }

  # Get html txt
  tempHtml <- tempUrl %>%
    GET(., timeout(30)) %>%
    read_html()

  tempFacilityTxt <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[3]") %>%
    html_text()

  tempTrackSize <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[4]") %>%
    html_text()

  tempTxt <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]/div[1]") %>%
    html_text()

  tempMeetName <- tempHtml %>%
    html_nodes(xpath = "/html/body/div[3]/div/div/div[1]/h3") %>%
    html_text()

  # Drop new line char, etc
  tempTxt <- gsub("[[:space:]]", "", tempTxt)

  # Get year
  tempYr <- case_when(
    grepl('2005', tempTxt) ~ '2005',
    grepl('2006', tempTxt) ~ '2006',
    grepl('2007', tempTxt) ~ '2007',
    grepl('2008', tempTxt) ~ '2008',
    grepl('2009', tempTxt) ~ '2009',
    grepl('2010', tempTxt) ~ '2010',
    grepl('2011', tempTxt) ~ '2011',
    grepl('2012', tempTxt) ~ '2012',
    grepl('2013', tempTxt) ~ '2013',
    grepl('2014', tempTxt) ~ '2014',
    grepl('2015', tempTxt) ~ '2015',
    grepl('2016', tempTxt) ~ '2016',
    grepl('2017', tempTxt) ~ '2017',
    grepl('2018', tempTxt) ~ '2018',
    grepl('2019', tempTxt) ~ '2019',
    grepl('2020', tempTxt) ~ '2020',
    grepl('2021', tempTxt) ~ '2021',
    grepl('2022', tempTxt) ~ '2022',
    grepl('2023', tempTxt) ~ '2023',
    T ~ 'OTHER'
  )

  # Get day number
  tempDay <- stri_extract_first_regex(tempTxt, "[0-9]+")

  # Get month
  tempMon <- case_when(
    grepl("Jan", tempTxt) ~ '1',
    grepl('Feb', tempTxt) ~ '2',
    grepl('Mar', tempTxt) ~ '3',
    grepl("Apr", tempTxt) ~ '4',
    grepl('May', tempTxt) ~ '5',
    grepl('Jun', tempTxt) ~ '6',
    grepl('Jul', tempTxt) ~ '7',
    grepl('Aug', tempTxt) ~ '8',
    grepl('Sep', tempTxt) ~ '9',
    grepl('Oct', tempTxt) ~ '10',
    grepl('Nov', tempTxt) ~ '11',
    grepl('Dec', tempTxt) ~ '12',
    T ~ '0'
  )

  # Combine into date
  tempDt <- paste0(tempYr, "-", tempMon, "-", tempDay)

  # Check for NULLs
  tempFacilityTxt <- ifelse(identical(tempFacilityTxt, character(0)), "no location", tempFacilityTxt)

  tempTrackSize <- ifelse(identical(tempTrackSize, character(0)), "no location", tempTrackSize)

  # Append to vectors
  meetNames <- append(meetNames, tempMeetName)
  meetDates <- append(meetDates, tempDt)
  meetFacs <- append(meetFacs, tempFacilityTxt)
  meetTrkSz <- append(meetTrkSz, tempTrackSize)
}

# Create data frame
meets <- as.data.frame(cbind(meetNames, meetDates, meetFacs, meetTrkSz))
names(meets) <- c("meet_name", "meet_date", "meet_facility", "meet_track_size")

meets <- meets %>%
  funique()

# Upload to a table
# dbRemoveTable(pg, "meet_dates")
# dbCreateTable(pg, "meet_dates", meets)
dbWriteTable(pg, "meet_dates", meets, append = TRUE)
