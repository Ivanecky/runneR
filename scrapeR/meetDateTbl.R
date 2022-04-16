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

# Performance list links
plLinks <- c("https://www.tfrrs.org/lists/2770/2019_2020_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2020/i",
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

plMeetLinks <- vector()

# Iterate over PL pages and get links
for (i in 1:length(plLinks)) {
  # Print progress
  print(paste0("Getting data for PL: ", i, " of ", length(plLinks)))
  # Get meet links from PL page
  tempPlLinks <- getPLMeetLinks(plLinks[i])
  # Append to vector
  plMeetLinks <- append(plMeetLinks, tempPlLinks)
}

# Remove duplicate links
plMeetLinks <- funique(plMeetLinks)

meetLinks <- dbGetQuery(pg, "select * from meet_links")

# Get rid of any doubles
meetLinks <- funique(meetLinks$links)

# Add PL meets to meets from tbl
meetLinks <- append(meetLinks, plMeetLinks)

# Get current meet links 
currentLinks <- getMeetLinks()
meetLinks <- append(meetLinks, currentLinks)

# Clean out links that are event specific
meetLinks <- meetLinks[!(grepl("_Jump|_Vault|meteres|Meters|_Relay|Hurdles|_Throw|Mile|Pentathlon|Heptathlon|Shot_Put|Discus|Hammer|Javelin|Decathlon|Steeplechase", meetLinks, ignore.case = TRUE))]

# Vectors to hold info
meetNames <- vector()
meetDates <- vector()
meetFacs <- vector()
meetTrkSz <- vector()
errorLinks <- vector()

# Iterate over meets
for (i in 1:length(meetLinks)) {
  # Print out which meet
  print(paste0("Getting data for meet ", meetLinks[i]))
  
  # Go through meets and get dates
  tempUrl <- meetLinks[i]
  
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
dbRemoveTable(pg, "meet_dates")
dbCreateTable(pg, "meet_dates", meets)
dbWriteTable(pg, "meet_dates", meets, append = TRUE)
