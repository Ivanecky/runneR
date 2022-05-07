# RUN THIS FOR TRACK MEET SCRAPING IN PARALLEL
# Code to generate line item performances from TFRRS in parallel process.

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
library(RCurl)
library(doParallel)

# Load functions for scraping
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")

# Connect to postgres
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

# Meet results URL
url <- "https://www.tfrrs.org/results_search.html"

# Read meet name links
links <- getMeetLinks(url)

# Create a links DF to upload links to AWS table, storing links for meets that have been scraped
linksDf <- as.data.frame(links)

# Query links from link table
linkTbl <- dbGetQuery(pg, "select * from meet_links")

# Get new links (not in table)
joinLinks <- linksDf %>%
  filter(!(links %in% linkTbl$links))

# Performance list URLs
plLinks <- c( # "https://www.tfrrs.org/lists/2770/2019_2020_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2020/i",
#                "https://www.tfrrs.org/lists/2771/2019_2020_NCAA_Div._II_Indoor_Qualifying_(FINAL)",
#                "https://www.tfrrs.org/lists/2772/2019_2020_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2020/i",
#                "https://www.tfrrs.org/archived_lists/2568/2019_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2019/o",
#                "https://www.tfrrs.org/archived_lists/2571/2019_NCAA_Div._II_Outdoor_Qualifying_(FINAL)/2019/o",
#                "https://www.tfrrs.org/archived_lists/2572/2019_NCAA_Div._III_Outdoor_Qualifying_(FINAL)/2019/o",
#                "https://www.tfrrs.org/archived_lists/2324/2018_2019_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2019/i",
#                "https://www.tfrrs.org/archived_lists/2325/2018_2019_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2019/i",
#                "https://www.tfrrs.org/archived_lists/2326/2018_2019_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2019/i",
#                "https://www.tfrrs.org/archived_lists/2279/2018_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2018/o",
#                "https://www.tfrrs.org/archived_lists/2282/2018_NCAA_Div._II_Outdoor_Qualifying_(FINAL)/2018/o",
#                "https://www.tfrrs.org/archived_lists/2283/2018_NCAA_Div._III_Outdoor_Qualifying_(FINAL)/2018/o",
#                "https://www.tfrrs.org/archived_lists/2124/2017_2018_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2018/i",
#                "https://www.tfrrs.org/archived_lists/2125/2017_2018_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2018/i",
#                "https://www.tfrrs.org/archived_lists/2126/2017_2018_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2018/i",
#                "https://www.tfrrs.org/archived_lists/1912/2017_NCAA_Div._I_Outdoor_Qualifying_(FINAL)/2017/o",
#                "https://www.tfrrs.org/archived_lists/1913/2017_NCAA_Div._II_Outdoor_Qualifying_(FINAL)/2017/o",
#                "https://www.tfrrs.org/archived_lists/1914/2017_NCAA_Div._III_Outdoor_Qualifying_(FINAL)/2017/o",
#                "https://www.tfrrs.org/archived_lists/1797/2016_2017_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2017/i",
#                "https://www.tfrrs.org/archived_lists/1798/2016_2017_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2017/i",
#                "https://www.tfrrs.org/archived_lists/1799/2016_2017_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2017/i",
#                "https://www.tfrrs.org/lists/3191/2021_NCAA_Division_I_Outdoor_Qualifying_(FINAL)/2021/o",
#                "https://www.tfrrs.org/lists/3194/2021_NCAA_Division_II_Outdoor_Qualifying_(FINAL)",
#                "https://www.tfrrs.org/lists/3195/2021_NCAA_Division_III_Outdoor_Qualifying_(FINAL)/2021/o",
#                "https://www.tfrrs.org/lists/3196/2021_NAIA_Outdoor_Qualifying_List_(FINAL)",
#                "https://www.tfrrs.org/lists/3157/2020_2021_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2021/i",
#                "https://www.tfrrs.org/lists/3158/2020_2021_NCAA_Div._II_Indoor_Qualifying_(FINAL)",
#                "https://www.tfrrs.org/lists/3161/2020_2021_NCAA_Division_III_Indoor_Qualifying_List/2021/i",
#                "https://www.tfrrs.org/lists/3156/2020_2021_NAIA_Indoor_Qualifying_(FINAL)/2021/i",
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
               "https://www.tfrrs.org/archived_lists/1026/2013_NAIA_Outdoor_Qualifying_List_(FINAL)/2013/o"
#                "https://www.tfrrs.org/archived_lists/942/2012_2013_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2013/i",
#                "https://www.tfrrs.org/archived_lists/943/2012_2013_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2013/i",
#                "https://www.tfrrs.org/archived_lists/944/2012_2013_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2013/i",
#                "https://www.tfrrs.org/archived_lists/945/2012_2013_NAIA_Indoor_Qualifying_List_(FINAL)/2013/i",
#                "https://www.tfrrs.org/archived_lists/840/2012_NCAA_Div._I_Outdoor_Qualifiers_(Final)/2012/o",
#                "https://www.tfrrs.org/archived_lists/841/2012_NCAA_Div._II_Outdoor_Qualifier_List_(Final)/2012/o",
#                "https://www.tfrrs.org/archived_lists/842/2012_NCAA_Div._III_Outdoor_Qualifier_List/2012/o",
#                "https://www.tfrrs.org/archived_lists/845/2012_NAIA_Outdoor_Qualifier_List_(FINAL/CLOSED)/2012/o",
#                "https://www.tfrrs.org/archived_lists/769/2011_2012_NCAA_Div._I_Indoor_Qualifiers_(FINAL)/2012/i",
#                "https://www.tfrrs.org/archived_lists/770/2011_12_NCAA_Div._II_Indoor_Qualifiers_(FINAL)/2012/i",
#                "https://www.tfrrs.org/archived_lists/771/2011_2012_NCAA_Div._III_Indoor_Qualifiers_(FINAL)/2012/i",
#                "https://www.tfrrs.org/archived_lists/772/2011_2012_NAIA_Indoor_Qualifier_List_(Final)/2012/i",
#                "https://www.tfrrs.org/archived_lists/673/2011_NCAA_Division_I_Outdoor_POP_List_(FINAL)/2011/o",
#                "https://www.tfrrs.org/archived_lists/674/2011_NCAA_Division_II_Outdoor_POP_List_(FINAL)/2011/o",
#                "https://www.tfrrs.org/archived_lists/675/2011_NCAA_Division_III_Outdoor_POP_List_(FINAL)/2011/o",
#                "https://www.tfrrs.org/archived_lists/676/2011_NAIA_Outdoor_POP_List_(FINAL/CLOSED)/2011/o",
#                "https://www.tfrrs.org/archived_lists/607/2010_2011_NCAA_Div._I_Indoor_POP_List_(Final)/2011/i",
#                "https://www.tfrrs.org/archived_lists/608/2010_2011_NCAA_Div._II_Indoor_POP_List_(Final)/2011/i",
#                "https://www.tfrrs.org/archived_lists/609/2010_2011_NCAA_Div._III_Indoor_POP_List_(Final)/2011/i",
#                "https://www.tfrrs.org/archived_lists/610/2010_2011_NAIA_Indoor_POP_List_(FINAL/CLOSED)/2011/i",
#                "https://www.tfrrs.org/archived_lists/528/2010_NCAA_Division_I_Outdoor_POP_List_(FINAL)/2010/o",
#                "https://www.tfrrs.org/archived_lists/529/2010_NCAA_Division_II_Outdoor_POP_List_(Final)/2010/o",
#                "https://www.tfrrs.org/archived_lists/530/2010_NCAA_Division_III_Outdoor_Track_&_Field/2010/o",
#                "https://www.tfrrs.org/archived_lists/541/2010_NAIA_Outdoor_Track_POP_List_(Final/Closed)/2010/o",
#                "https://www.tfrrs.org/archived_lists/502/2009_2010_NCAA_Div._I_Indoor_POP_List_(FINAL)/2010/i",
#                "https://www.tfrrs.org/archived_lists/503/2009_2010_NCAA_Div._II_Indoor_POP_List_(Final)/2010/i",
#                "https://www.tfrrs.org/archived_lists/504/2009_10_NCAA_Division_III_Indoor_Track_&_Field/2010/i",
#                "https://www.tfrrs.org/archived_lists/476/2009_2010_NAIA_Indoor_POP_List_(Final/Closed)/2010/i",
#                # Current lists
#                "https://www.tfrrs.org/lists/3492/2021_2022_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2022/i",
#                "https://www.tfrrs.org/lists/3493/2021_2022_NCAA_Div._II_Indoor_Qualifying_(FINAL)",
#                "https://www.tfrrs.org/lists/3494/2021_2022_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2022/i",
#                "https://www.tfrrs.org/lists/3495/2021_2022_NAIA_Indoor_Qualifying_(FINAL)",
#                "https://www.tfrrs.org/lists/3711/2022_NCAA_Division_I_Outdoor_Qualifying_List/2022/o",
#                "https://www.tfrrs.org/lists/3595/2022_NCAA_Division_II_Outdoor_Qualifying_List",
#                "https://www.tfrrs.org/lists/3714/2022_NCAA_Division_III_Outdoor_Qualifying_List/2022/o",
#                "https://www.tfrrs.org/lists/3596/2022_NAIA_Outdoor_Qualifying_List"
  )
# 
# Hold PL meet links
plMeetLinks <- c()

for ( i in 1:length(plLinks)) {
  # Print status
  print(paste0("Getting data for: ", plLinks[i]))

  # Get meet URLsY
  meetUrls <- getPLMeetLinks(plLinks[i])

  # Append
  plMeetLinks <- append(plMeetLinks, meetUrls)
}

joinLinks <- joinLinks[!(grepl("_Jump|_Vault|meteres|Meters|Hurdles|_Throw|Mile|Pentathlon|Heptathlon|Shot_Put|Discus|Hammer|Javelin|Decathlon|Steeplechase", joinLinks, ignore.case = TRUE))]
joinLinks <- joinLinks[!(grepl("_Relay", joinLinks, ignore.case = TRUE) & !grepl("_Relays", joinLinks, ignore.case = TRUE))]

# Write data to table for URLs
#dbRemoveTable(pg, "meet_links")
#dbCreateTable(pg, "meet_links", linksDf)
dbWriteTable(pg, "meet_links", joinLinks, append = TRUE)

# Convert back to vector
joinLinks <- joinLinks$links

# Vector to hold runner URLs
runnerLinks <- vector()

# Vector to hold links that threw an error
meetErrLinks <- vector()

# Try running in parallel
runnerLinks <- getParRunnerURLs(joinLinks)

# Get unqiue runners
runnerLinks <- funique(runnerLinks)

# Get runner data
# Run code in parallel
runner_lines_res <- runnerResQueryV2(runnerLinks)

# Upload to AWS database
# Pull current data out of table
currentData <- dbGetQuery(pg, "select * from runner_line_item_raw") %>%
  mutate(
    RUNNER_KEY = paste0(NAME, "-", GENDER, "-", TEAM)
  ) 

# Modifying data before loading
runRecs <- runner_lines_res %>%
  as.data.frame() %>%
  filter(MEET_NAME != "meet") %>%
  funique() %>%
  mutate(
    PLACE = as.numeric(PLACE),
    NAME = gsub("[^\x01-\x7F]", "", NAME)
  ) %>%
  mutate(
    RUNNER_KEY = paste0(NAME, "-", GENDER, "-", TEAM)
  )

# Remove runners who are in the new data
currentData <- currentData %>%
  filter(!(RUNNER_KEY %in% runRecs$RUNNER_KEY))

# Join data from old & new
uploadData <- plyr::rbind.fill(runRecs, currentData) %>%
  funique() %>%
  mutate(
    load_d = lubridate::today(),
    MEET_DATE = gsub(",", "", MEET_DATE)
  ) %>%
  filter(EVENT != "OTHER")

# Upload runner data to table
# Write data to table for URLs
# dbRemoveTable(aws, "race_results")
# dbCreateTable(pg, "runner_line_item_raw", runRecs)
dbWriteTable(pg, "runner_line_item_raw", uploadData, overwrite = TRUE)
