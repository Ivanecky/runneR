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
library(plyr)
library(yaml)

# Source file for functions
source("/Users/samivanecky/git/runneR//scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/altConversinFxns.R")

# Define functions
getMeetDate <- function(dets) {
  return(unlist(str_split(dets, "\\|"))[1])
}

getMeetFacility <- function(dets) {
  return(unlist(str_split(dets, "\\|"))[2])
}

getMeetTrackSize <- function(dets) {
  # Get track size
  track_size = case_when(
    length(unlist(str_split(dets, "\\|"))) == 4 ~ unlist(str_split(dets, "\\|"))[3],
    length(unlist(str_split(dets, "\\|"))) == 3 & grepl("m", unlist(str_split(dets, "\\|"))[3]) ~ unlist(str_split(dets, "\\|"))[3],
    T ~ ''
  )
  # Return value
  return(track_size)
}

getMeetElevation <- function(dets) {
  # Get elevation
  elevation = case_when(
    length(unlist(str_split(dets, "\\|"))) == 4 ~ unlist(str_split(dets, "\\|"))[4],
    length(unlist(str_split(dets, "\\|"))) == 3 & grepl("elevation", unlist(str_split(dets, "\\|"))[3] ) ~ unlist(str_split(dets, "\\|"))[3],
    T ~ ''
  )
  # Return
  return(elevation)
}

# Meet results URL
url <- "https://www.tfrrs.org/results_search.html"

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

# Read meet name links
meetLinks <- getMeetLinks(url)

# Query old meet links
# meetLinks <- dbGetQuery(pg, "select distinct * from meet_links")

# Remove any dups
meet_links <- funique(meetLinks$links)

# Clean out links that are event specific
meet_links <- meet_links[!(grepl("_Jump|_Vault|meteres|Meters|Hurdles|_Throw|Mile|Pentathlon|Heptathlon|Shot_Put|Discus|Hammer|Javelin|Decathlon|Steeplechase", meet_links, ignore.case = TRUE))]
meet_links <- meet_links[!(grepl("_Relay", meet_links, ignore.case = TRUE) & !grepl("_Relays", meet_links, ignore.case = TRUE))]

# Iterate over meet links and get data
meet_dets <- vector()
meet_names <- vector()

for(i in 1:length(meet_links)) {
  # Print status
  print(paste0("Getting data for ", i, " of ", length(meet_links)))
  
  # Try and get meet info data
  temp_txt_dets <- tryCatch({
      # Get html text
      temp_txt <- meet_links[i] %>%
        read_html() %>%
        html_node(xpath = "/html/body/div[3]/div/div/div[2]/div/div[1]/div[1]") %>%
        html_text()
      
      # Clean up
      temp_txt <- str_squish(gsub("[\t\n]", " ", temp_txt))
    },  
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
      # Sys.sleep(60)
      return(NA)
    }
  )
  
  # Try and get meet name data
  temp_txt_name <- tryCatch({
      # Get html text
      temp_txt <- meet_links[i] %>%
        read_html() %>%
        html_node(xpath = "/html/body/div[3]/div/div/div[1]/h3") %>%
        html_text()
      
      # Clean up
      temp_txt <- str_squish(gsub("[\t\n]", " ", temp_txt))
    },  
    error=function(cond) {
      message("Here's the original error message:")
      message(cond)
      # Sys.sleep(60)
      return(NA)
    }
  )
  
  # Append
  meet_dets <- append(meet_dets, temp_txt_dets)
  meet_names <- append(meet_names, temp_txt_name)
}

# Bind vectors as dataframe
meets <- as.data.frame(cbind(meet_names, meet_dets))
names(meets) <- c("name", "dets")

# Clean data
meets$meet_date = sapply(meets$dets, getMeetDate)
meets$facility = sapply(meets$dets, getMeetFacility)
meets$track_size = sapply(meets$dets, getMeetTrackSize)
meets$elevation = sapply(meets$dets, getMeetElevation)

# Add data for banked/flat and track sizes
meets <- meets %>%
  mutate(
    banked_or_flat = case_when(
      grepl("unm", facility, ignore.case = T) ~ "banked",
      grepl("black hills st", facility, ignore.case = T) ~ "flat",
      grepl("air force", facility, ignore.case = T) ~ "flat",
      grepl("montana state", facility, ignore.case = T) ~ "flat",
      grepl("colorado mines", facility, ignore.case = T) ~ "flat",
      grepl("utah state", facility, ignore.case = T) ~ "banked",
      grepl("weber state", facility, ignore.case = T) ~ "flat",
      grepl("idaho state", facility, ignore.case = T) ~ "banked",
      grepl("texas tech", facility, ignore.case = T) ~ "banked",
      grepl("western state", facility, ignore.case = T) ~ "flat",
      grepl("utah valley", facility, ignore.case = T) ~ "flat",
      grepl("montana - missoula", facility, ignore.case = T) ~ "flat",
      grepl("colorado st", facility, ignore.case = T) ~ "flat",
      grepl("northern colorado", facility, ignore.case = T) ~ "flat",
      grepl("utep", facility, ignore.case = T) ~ "flat",
      grepl("eastern new mexico", facility, ignore.case = T) ~ "flat",
      grepl("west texas a&m", facility, ignore.case = T) ~ "flat",
      grepl("csu-pueblo", facility, ignore.case = T) ~ "flat",
      grepl("laramie, wy", facility, ignore.case = T) ~ "flat",
      grepl("banked", track_size, ignore.case = T) ~ "banked",
      T ~ "flat"
    )
  ) %>%
  mutate(
    track_size = case_when(
      grepl("byu", facility, ignore.case = T) ~ "300m",
      grepl("boulder", facility, ignore.case = T) ~ "300m",
      grepl("unm", facility, ignore.case = T) ~ "200m",
      grepl("boulder", facility, ignore.case = T) ~ "300m",
      grepl("black hills st", facility, ignore.case = T) ~ "200m",
      grepl("air force", facility, ignore.case = T) ~ "268m",
      grepl("appalachian", facility, ignore.case = T) ~ "300m",
      grepl("montana state", facility, ignore.case = T) ~ "200m",
      grepl("colorado mines", facility, ignore.case = T) ~ "193m",
      grepl("utah state", facility, ignore.case = T) ~ "200m",
      grepl("weber state", facility, ignore.case = T) ~ "200m",
      grepl("idaho state", facility, ignore.case = T) ~ "200m",
      grepl("texas tech", facility, ignore.case = T) ~ "200m",
      grepl("western state", facility, ignore.case = T) ~ "200m",
      grepl("utah valley", facility, ignore.case = T) ~ "400m",
      grepl("montana - missoula", facility, ignore.case = T) ~ "400m",
      grepl("colorado st", facility, ignore.case = T) ~ "400m",
      grepl("northern colorado", facility, ignore.case = T) ~ "400m",
      grepl("utep", facility, ignore.case = T) ~ "400m",
      grepl("eastern new mexico", facility, ignore.case = T) ~ "400m",
      grepl("west texas a&m", facility, ignore.case = T) ~ "400m",
      grepl("csu-pueblo", facility, ignore.case = T) ~ "400m",
      grepl("laramie, wy", facility, ignore.case = T) ~ "160m",
      grepl("300m", track_size, ignore.case = T) ~ "300m",
      grepl("200m", track_size, ignore.case = T) ~ "200m",
      T ~ "400m"
    )
  ) 

# Correct elevation
meets <- meets %>%
  mutate(
    elevation = case_when(
      grepl("alamosa", facility, ignore.case = TRUE) ~ "7545",
      grepl("air force", facility, ignore.case = TRUE) ~ "6981",
      grepl("appalachian state", facility, ignore.case = TRUE) ~ "3333",
      grepl("black hills st", facility, ignore.case = TRUE) ~ "3593",
      grepl("byu", facility, ignore.case = TRUE) ~ "7545",
      grepl("golden, co", facility, ignore.case = TRUE) ~ "5675",
      grepl("boulder, co", facility, ignore.case = TRUE) ~ "5260",
      grepl("pocatello, id", facility, ignore.case = TRUE) ~ "4465",
      grepl("bozeman, mt", facility, ignore.case = TRUE) ~ "4926",
      grepl("texas tech", facility, ignore.case = TRUE) ~ "3281",
      grepl("logan, ut", facility, ignore.case = TRUE) ~ "4680",
      grepl("ogden, ut", facility, ignore.case = TRUE) ~ "4759",
      grepl("laramie, wy", facility, ignore.case = TRUE) ~ "7163",
      T ~ gsub("ft. elevation", "", elevation)
    )
  )

# Clean up fields, extract date components
meets <- meets %>%
  mutate(
    name = trimws(name),
    dets = trimws(dets),
    facility = trimws(facility),
    meet_date = trimws(meet_date),
    track_size = trimws(track_size),
    elevation = trimws(elevation),
    banked_or_flat = trimws(banked_or_flat)
  ) %>%
  mutate(
    month = case_when(
      grepl("January", meet_date, ignore.case = TRUE) ~ "1",
      grepl("February", meet_date, ignore.case = TRUE) ~ "2",
      grepl("March", meet_date, ignore.case = TRUE) ~ "3",
      grepl("April", meet_date, ignore.case = TRUE) ~ "4",
      grepl("May", meet_date, ignore.case = TRUE) ~ "5",
      grepl("June", meet_date, ignore.case = TRUE) ~ "6",
      grepl("July", meet_date, ignore.case = TRUE) ~ "7",
      grepl("August", meet_date, ignore.case = TRUE) ~ "8",
      grepl("September", meet_date, ignore.case = TRUE) ~ "9",
      grepl("October", meet_date, ignore.case = TRUE) ~ "10",
      grepl("Novemeber", meet_date, ignore.case = TRUE) ~ "11",
      grepl("December", meet_date, ignore.case = TRUE) ~ "12",
      T ~ 'OTHER'
    ),
    year = trimws(substr(meet_date, nchar(meet_date) - 4, nchar(meet_date))),
    day = trimws(stri_extract_first_regex(meet_date, "[0-9]+"))
  ) %>%
  mutate(
    race_date = lubridate::ymd(paste0(year, "-", month, "-", day))
  ) %>%
  select(
    -c(month, day, year)
  ) %>%
  mutate(
    load_d = lubridate::today()
  )

# Write data to table
# dbCreateTable(pg, "meet_lookup_info", meets)
dbWriteTable(pg, "meet_lookup_info", meets, append = TRUE)
