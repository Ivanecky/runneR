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

# Load functions for scraping
source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/TrackPowerRankings/scrapeR/ResultsQuery.R")

# Connect to AWS
# Read connection data from yaml
aws.yml <- read_yaml("/Users/samivanecky/git/TrackPowerRankings/aws.yaml")

# Get performance list URLs
# D1
indoorD111 <- "https://www.tfrrs.org/archived_lists/607/2010_2011_NCAA_Div._I_Indoor_POP_List_(Final)/2011/i"
indoorD112 <- "https://www.tfrrs.org/archived_lists/769/2011_2012_NCAA_Div._I_Indoor_Qualifiers_(FINAL)/2012/i"
indoorD113 <- "https://www.tfrrs.org/archived_lists/942/2012_2013_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2013/i"
indoorD114 <- "https://www.tfrrs.org/archived_lists/1139/2013_2014_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2014/i"
indoorD115 <- "https://www.tfrrs.org/archived_lists/1345/2014_2015_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2015/i"
indoorD116 <- "https://www.tfrrs.org/archived_lists/1569/2015_2016_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2016/i"
indoorD117 <- "https://www.tfrrs.org/archived_lists/1797/2016_2017_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2017/i"
indoorD118 <- "https://www.tfrrs.org/archived_lists/2124/2017_2018_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2018/i"
indoorD119 <- "https://www.tfrrs.org/archived_lists/2324/2018_2019_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2019/i"
indoorD120 <- "https://www.tfrrs.org/lists/2770/2019_2020_NCAA_Div._I_Indoor_Qualifying_(FINAL)/2020/i"
indoorD121 <- "https://www.tfrrs.org/lists/3157/2020_2021_NCAA_Division_I_Indoor_Qualifying_List/2021/i"

# D2
# indoorD211 <- "https://www.tfrrs.org/archived_lists/608/2010_2011_NCAA_Div._II_Indoor_POP_List_(Final)/2011/i"
# indoorD212 <- "https://www.tfrrs.org/archived_lists/770/2011_12_NCAA_Div._II_Indoor_Qualifiers_(FINAL)/2012/i"
# indoorD213 <- "https://www.tfrrs.org/archived_lists/943/2012_2013_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2013/i"
# indoorD214 <- "https://www.tfrrs.org/archived_lists/1140/2013_2014_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2014/i"
# indoorD215 <- "https://www.tfrrs.org/archived_lists/1347/2014_2015_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2015/i"
# indoorD216 <- "https://www.tfrrs.org/archived_lists/1570/2015_2016_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2016/i"
# indoorD217 <- "https://www.tfrrs.org/archived_lists/1798/2016_2017_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2017/i"
# indoorD218 <- "https://www.tfrrs.org/archived_lists/2125/2017_2018_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2018/i"
# indoorD219 <- "https://www.tfrrs.org/archived_lists/2325/2018_2019_NCAA_Div._II_Indoor_Qualifying_(FINAL)/2019/i"
# indoorD220 <- "https://www.tfrrs.org/lists/2771/2019_2020_NCAA_Div._II_Indoor_Qualifying_(FINAL)"
# indoorD221 <- "https://www.tfrrs.org/lists/3158/2020_2021_NCAA_Division_II_Indoor_Qualifying_List/2021/i"

# D3
# indoorD311 <- "https://www.tfrrs.org/archived_lists/609/2010_2011_NCAA_Div._III_Indoor_POP_List_(Final)/2011/i"
# indoorD312 <- "https://www.tfrrs.org/archived_lists/771/2011_2012_NCAA_Div._III_Indoor_Qualifiers_(FINAL)/2012/i"
# indoorD313 <- "https://www.tfrrs.org/archived_lists/944/2012_2013_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2013/i"
# indoorD314 <- "https://www.tfrrs.org/archived_lists/1141/2013_2014_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2014/i"
# indoorD315 <- "https://www.tfrrs.org/archived_lists/1353/2014_2015_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2015/i"
# indoorD316 <- "https://www.tfrrs.org/archived_lists/1571/2015_2016_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2016/i"
# indoorD317 <- "https://www.tfrrs.org/archived_lists/1799/2016_2017_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2017/i"
# indoorD318 <- "https://www.tfrrs.org/archived_lists/2126/2017_2018_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2018/i"
# indoorD319 <- "https://www.tfrrs.org/archived_lists/2326/2018_2019_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2019/i"
# indoorD320 <- "https://www.tfrrs.org/lists/2772/2019_2020_NCAA_Div._III_Indoor_Qualifying_(FINAL)/2020/i"
# indoorD321 <- "https://www.tfrrs.org/lists/3161/2020_2021_NCAA_Division_III_Indoor_Qualifying_List/2021/i"

# Set URLs to be scraped
# D1
outdoorD121 <- "https://www.tfrrs.org/lists/3191/2021_NCAA_Division_I_Outdoor_Qualifying/2021/o"

# D2
outdoorD221 <- "https://www.tfrrs.org/lists/3194/2021_NCAA_Division_II_Outdoor_Qualifying"

# D3
outdoorD321 <- "https://www.tfrrs.org/lists/3195/2021_NCAA_Division_III_Outdoor_Qualifying/2021/o"

# Create list of URLs
urls <- c(indoorD117, indoorD118, indoorD119, indoorD120, indoorD121, indoorD111, indoorD112, indoorD113, indoorD114, indoorD115, indoorD116)

# Athlete URLs
athletes <- c()

# Loop over urls and create list of athlete URLs
for ( i in 1:length(urls) )
{
  meet_wp <- getPerfListURLs(urls[i])
  # Subset to distance events
  wp <- meet_wp
  #wp <- c(meet_wp[601:1400], meet_wp[2001:2200])
  # Remove duplicates
  wp <- unique(wp)
  # Append to athletes
  athletes <- c(athletes, wp)
}

# Drop duplicates from athletes
athletes <- unique(athletes)

# Get data for runners
runner_line_items <- resultsQuery(athletes)

# Fix whitespace in names
runner_line_items <- runner_line_items %>%
  filter(EVENT != 'OTHER') %>%
  mutate(
    NAME = str_squish(NAME), 
    TIME = as.numeric(TIME),
    PLACE = as.numeric(PLACE)
  )

# Get grouped data for runners
runners_grouped <- groupedResults(runner_line_items)

# Get grouped yearly results
runners_grouped_yr <- groupedYearlyResults(runner_line_items)

# Reformat data
#df_format <- reformatRunners(runners_grouped)
#yr_format <- reformatYearlyRunners(runners_grouped_yr)

# Connect to database
aws <- dbConnect(
  RPostgres::Postgres(),
  host = aws.yml$host,
  user = aws.yml$user,
  password = aws.yml$password,
  port = aws.yml$port
)

# Write data to table
#dbRemoveTable(aws, "runners_grouped")
#dbCreateTable(aws, "runners_grouped", runners_grouped)
dbWriteTable(aws, "runners_grouped", runners_grouped, append = TRUE)

#dbRemoveTable(aws, "runners_grouped_yearly")
#dbCreateTable(aws, "runners_grouped_yearly", runners_grouped_yr)
dbWriteTable(aws, "runners_grouped_yearly", runners_grouped_yr, append = TRUE)

# Write line items to table
#dbRemoveTable(aws, "performance_lines")
#dbCreateTable(aws, "performance_lines", runner_line_items)
dbWriteTable(aws, "performance_lines", runner_line_items, append = TRUE)

######################################################################## 
# Code to read TFRRS lists for altitude conversions
# getConversions <- function(plURL) {
#   # Accountinf for conversions
#   pl <- read_html(plURL) %>%
#     html_table()
#   
#   for(i in 7:14)
#   {
#     results <- as.data.frame(pl[i])
#     
#     # Subset to only conversion times
#     results <- results %>%
#       select(ATHLETE, TEAM, TIME) %>%
#       filter(grepl("[#|@]", TIME)) %>%
#       mutate(
#         YEAR = '2021',
#         TEAM = toupper(TEAM),
#         GENDER = case_when(
#           i %% 2 == 0 ~ 'F',
#           T ~ 'M'
#         ),
#         EVENT = case_when(
#           i == 7 | i == 8 ~ '800m',
#           i == 9 | i == 10 ~ 'Mile',
#           i == 11 | i == 12 ~ '3000m',
#           T ~ '5000m'
#         ),
#         PLACE = NA
#       )
#     
#     # Handle times
#     results$TIME = sapply(results$TIME, handleTimes)
#     results$NAME = sapply(results$ATHLETE, createName)
#     
#     # Rearrange columns
#     results <- results %>%
#       select(YEAR, EVENT, TIME, PLACE, NAME, GENDER, TEAM)
#   
#     if(exists("full_results"))
#     {
#       full_results <- rbind(full_results, results)
#     }
#     else
#     {
#       full_results <- results  
#     }
#     
#   }
#   
#   # Return data
#   return(full_results)
#   
# }
# 
# # Run for indoor list
# plURL <- outdoorD221
# df <- getConversions(plURL)
# 
# # Upload to DB
# dbWriteTable(aws, "performance_lines", df, append = TRUE)
