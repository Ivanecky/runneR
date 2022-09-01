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

# Performance list URLs
plLinks <- c( "https://www.tfrrs.org/lists/2770/2019_2020_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2020/i",
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
   "https://www.tfrrs.org/lists/3156/2020_2021_NAIA_Indoor_Qualifying_(FINAL)/2021/i",
  "https://www.tfrrs.org/archived_lists/1688/2016_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2016/o",
  "https://www.tfrrs.org/archived_lists/1685/2016_NCAA_Division_II_Outdoor_Qualifying_(FINAL)/2016/o",
  "https://www.tfrrs.org/archived_lists/1684/2016_NCAA_Division_III_Outdoor_Qualifying_(FINAL)/2016/o",
  "https://www.tfrrs.org/archived_lists/1662/2016_NAIA_Outdoor_Qualifying_List_(FINAL)/2016/o",
  "https://www.tfrrs.org/archived_lists/1569/2015_2016_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2016/i",
  "https://www.tfrrs.org/archived_lists/1570/2015_2016_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2016/i",
  "https://www.tfrrs.org/archived_lists/1571/2015_2016_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2016/i",
  "https://www.tfrrs.org/archived_lists/1576/2015_2016_NAIA_Indoor_Qualifying_List_(FINAL)/2016/i",
  "https://www.tfrrs.org/archived_lists/1439/2015_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2015/o",
  "https://www.tfrrs.org/archived_lists/1442/2015_NCAA_Division_II_Outdoor_Qualifying_(FINAL)/2015/o",
  "https://www.tfrrs.org/archived_lists/1443/2015_NCAA_Division_III_Outdoor_Qualifying_(FINAL)/2015/o",
  "https://www.tfrrs.org/archived_lists/1438/2015_NAIA_Outdoor_Qualifying_List_(FINAL)/2015/o",
  "https://tfrrs.org/archived_lists/1345/2014_2015_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2015/i",
  "https://www.tfrrs.org/archived_lists/1347/2014_2015_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2015/i",
  "https://www.tfrrs.org/archived_lists/1353/2014_2015_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2015/i",
  "https://www.tfrrs.org/archived_lists/1349/2014_2015_NAIA_Indoor_Qualifying_List_(FINAL)/2015/i",
  "https://www.tfrrs.org/archived_lists/1228/2014_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2014/o",
  "https://www.tfrrs.org/archived_lists/1231/2014_NCAA_Division_II_Outdoor_Qualifying_(FINAL)/2014/o",
  "https://www.tfrrs.org/archived_lists/1232/2014_NCAA_Division_III_Outdoor_Qualifying_(FINAL)/2014/o",
  "https://www.tfrrs.org/archived_lists/1227/2014_NAIA_Outdoor_Qualifying_List_(FINAL)/2014/o",
  "https://www.tfrrs.org/archived_lists/1139/2013_2014_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2014/i",
  "https://www.tfrrs.org/archived_lists/1140/2013_2014_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2014/i",
  "https://www.tfrrs.org/archived_lists/1141/2013_2014_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2014/i",
  "https://www.tfrrs.org/archived_lists/1142/2013_2014_NAIA_Indoor_Qualifying_(FINAL)/2014/i",
  "https://www.tfrrs.org/archived_lists/1029/2013_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2013/o",
  "https://www.tfrrs.org/archived_lists/1032/2013_NCAA_Division_II_Outdoor_Qualifying_(FINAL)/2013/o",
  "https://www.tfrrs.org/archived_lists/1033/2013_NCAA_Division_III_Outdoor_Qualifying_(FINAL)/2013/o",
  "https://www.tfrrs.org/archived_lists/1026/2013_NAIA_Outdoor_Qualifying_List_(FINAL)/2013/o",
  "https://www.tfrrs.org/archived_lists/942/2012_2013_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2013/i",
  "https://www.tfrrs.org/archived_lists/943/2012_2013_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2013/i",
  "https://www.tfrrs.org/archived_lists/944/2012_2013_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2013/i",
  "https://www.tfrrs.org/archived_lists/945/2012_2013_NAIA_Indoor_Qualifying_List_(FINAL)/2013/i",
  "https://www.tfrrs.org/archived_lists/840/2012_NCAA_Div._I_Outdoor_Qualifiers_(Final)/2012/o",
  "https://www.tfrrs.org/archived_lists/841/2012_NCAA_Div._II_Outdoor_Qualifier_List_(Final)/2012/o",
  "https://www.tfrrs.org/archived_lists/842/2012_NCAA_Div._III_Outdoor_Qualifier_List/2012/o",
  "https://www.tfrrs.org/archived_lists/845/2012_NAIA_Outdoor_Qualifier_List_(FINAL/CLOSED)/2012/o",
  "https://www.tfrrs.org/archived_lists/769/2011_2012_NCAA_Div._I_Indoor_Qualifiers_(FINAL)/2012/i",
  "https://www.tfrrs.org/archived_lists/770/2011_12_NCAA_Div._II_Indoor_Qualifiers_(FINAL)/2012/i",
  "https://www.tfrrs.org/archived_lists/771/2011_2012_NCAA_Div._III_Indoor_Qualifiers_(FINAL)/2012/i",
  "https://www.tfrrs.org/archived_lists/772/2011_2012_NAIA_Indoor_Qualifier_List_(Final)/2012/i",
  "https://www.tfrrs.org/archived_lists/673/2011_NCAA_Division_I_Outdoor_POP_List_(FINAL)/2011/o",
  "https://www.tfrrs.org/archived_lists/674/2011_NCAA_Division_II_Outdoor_POP_List_(FINAL)/2011/o",
  "https://www.tfrrs.org/archived_lists/675/2011_NCAA_Division_III_Outdoor_POP_List_(FINAL)/2011/o",
  "https://www.tfrrs.org/archived_lists/676/2011_NAIA_Outdoor_POP_List_(FINAL/CLOSED)/2011/o",
  "https://www.tfrrs.org/archived_lists/607/2010_2011_NCAA_Div._I_Indoor_POP_List_(Final)/2011/i",
  "https://www.tfrrs.org/archived_lists/608/2010_2011_NCAA_Div._II_Indoor_POP_List_(Final)/2011/i",
  "https://www.tfrrs.org/archived_lists/609/2010_2011_NCAA_Div._III_Indoor_POP_List_(Final)/2011/i",
  "https://www.tfrrs.org/archived_lists/610/2010_2011_NAIA_Indoor_POP_List_(FINAL/CLOSED)/2011/i",
  "https://www.tfrrs.org/archived_lists/528/2010_NCAA_Division_I_Outdoor_POP_List_(FINAL)/2010/o",
  "https://www.tfrrs.org/archived_lists/529/2010_NCAA_Division_II_Outdoor_POP_List_(Final)/2010/o",
  "https://www.tfrrs.org/archived_lists/530/2010_NCAA_Division_III_Outdoor_Track_&_Field/2010/o",
  "https://www.tfrrs.org/archived_lists/541/2010_NAIA_Outdoor_Track_POP_List_(Final/Closed)/2010/o",
  "https://www.tfrrs.org/archived_lists/502/2009_2010_NCAA_Div._I_Indoor_POP_List_(FINAL)/2010/i",
  "https://www.tfrrs.org/archived_lists/503/2009_2010_NCAA_Div._II_Indoor_POP_List_(Final)/2010/i",
  "https://www.tfrrs.org/archived_lists/504/2009_10_NCAA_Division_III_Indoor_Track_&_Field/2010/i",
  "https://www.tfrrs.org/archived_lists/476/2009_2010_NAIA_Indoor_POP_List_(Final/Closed)/2010/i",
   # Current lists
   "https://www.tfrrs.org/lists/3492/2021_2022_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2022/i",
   "https://www.tfrrs.org/lists/3493/2021_2022_NCAA_Div._II_Indoor_Qualifying_(FINAL)",
   "https://www.tfrrs.org/lists/3494/2021_2022_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2022/i",
   "https://www.tfrrs.org/lists/3495/2021_2022_NAIA_Indoor_Qualifying_(FINAL)",
   "https://www.tfrrs.org/lists/3711/2022_NCAA_Division_I_Outdoor_Qualifying_List/2022/o",
   "https://www.tfrrs.org/lists/3595/2022_NCAA_Division_II_Outdoor_Qualifying_List",
   "https://www.tfrrs.org/lists/3714/2022_NCAA_Division_III_Outdoor_Qualifying_List/2022/o",
   "https://www.tfrrs.org/lists/3596/2022_NAIA_Outdoor_Qualifying_List"
)

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

meetLinks <- plMeetLinks

meetLinks <- dbGetQuery(pg, "select * from meet_links")

# Get rid of any doubles
meetLinks <- funique(meetLinks)

# Add PL meets to meets from tbl
meetLinks <- append(meetLinks, plMeetLinks)

# Get current meet links 
currentLinks <- getMeetLinks()
meetLinks <- append(meetLinks, currentLinks)

# Remove dups
meetLinks <- funique(meetLinks)

# Clean out links that are event specific
meetLinks <- meetLinks[!(grepl("_Jump|_Vault|meteres|Meters|Hurdles|_Throw|Mile|Pentathlon|Heptathlon|Shot_Put|Discus|Hammer|Javelin|Decathlon|Steeplechase", meetLinks, ignore.case = TRUE))]
# meetLinks <- meetLinks[!(grepl("_Relay", meetLinks, ignore.case = TRUE) & !grepl("_Relays", meetLinks, ignore.case = TRUE))]

# Parallel code test
meet_info <- getTrackMeetInfoPar(meetLinks)

# Remove duplicates and trim whitespace
meets <- meet_info %>% 
  funique() %>%
  mutate(
    MEET_NAME = trimws(MEET_NAME),
    MEET_DATE = trimws(MEET_DATE),
    MEET_FACILITY = trimws(MEET_FACILITY),
    MEET_TRACK_SZ = trimws(MEET_TRACK_SZ)
  )

# Current meet dates
current_meets <- dbGetQuery(pg, "select * from meet_dates")

# Combine and remove duplicates
new_meets <- rbind(meets, current_meets)
new_meets <- new_meets %>%
  funique()

# Upload to a table
dbRemoveTable(pg, "meet_dates")
dbCreateTable(pg, "meet_dates", new_meets)
dbWriteTable(pg, "meet_dates", new_meets, overwrite = TRUE)
