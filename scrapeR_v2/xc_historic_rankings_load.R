# Scrape current USTFCCCA Rankings
library(tidyverse)
library(RPostgreSQL)
library(DBI)
library(reshape2)
library(stringr)
library(stringi)
library(yaml)
library(zoo)
library(kit)

# Source file for functions
source("/Users/samivanecky/git/runneR/scrapeR/Scraping_Fxns.R")
source("/Users/samivanecky/git/runneR/scrapeR/meetScrapingFxns.R")

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

####################
### Functions
####################
getHistoricNationalRankings <- function(url, gender, season, div, type = "national") {
  # Create base URL
  url <- paste0(url, season)
  
  # Print status
  print(paste0("Getting data for ", url))
    
  # Tbls
  tbls <- url %>%
    GET(., timeout(30), user_agent(randUsrAgnt())) %>%
    read_html() %>%
    html_table()
  
  # Read table
  df <- as.data.frame(tbls[1])
  
  # Convert fields to numeric
  df[,2:ncol(df)] <- sapply(df[,2:ncol(df)], as.numeric)
  
  # Add fields
  df <- df %>%
    mutate(
      gender = gender,
      div = div,
      type = type,
      load_dt = lubridate::today()
    )
  
  # Return df
  return(df)
}

getHistoricRegionalRankings <- function(url, gender, season, div, type = "regional") {
  # Create URL
  url <- paste0(url, season)
  
  ## Read tbls
  df_tbls <- url %>%
    GET(., timeout(30), user_agent(randUsrAgnt())) %>%
    read_html() %>%
    html_table()
  
  # Number of regions differs on division, set tbls by division
  # n_tbls <- case_when(
  #   div == "D1" ~ 1,
  #   div == "D2" ~ 1,
  #   div == "D3" ~ 1,
  #   T ~ 0
  # )
  
  # Check if any tables exist
  if (length(df_tbls) <= 1)
    return(NULL)
  
  # Drop last tbls
  df_tbls <- df_tbls[1:(length(df_tbls) - 1)]
  
  # Read header attrs
  df_hdrs <- url %>%
    read_html() %>%
    html_nodes("h6") %>%
    html_text()
  
  # Iterate over each tbl
  for (i in 1:length(df_tbls)) {
    # Check if tbl exists
    if(exists("reg_df"))  {
      # Create data frane
      temp_df <- as.data.frame(df_tbls[i])
      # Convert fields to numeric
      temp_df[,2:ncol(temp_df)] <- sapply(temp_df[,2:ncol(temp_df)], as.numeric)
      # Add region
      temp_df <- temp_df %>%
        mutate(
          region = df_hdrs[i]
        )
      # Bind to existing data
      reg_df <- rbind(reg_df, temp_df)
    } else {
      # Create df
      reg_df <- as.data.frame(df_tbls[i])
      # Convert fields to numeric
      reg_df[,2:ncol(reg_df)] <- sapply(reg_df[,2:ncol(reg_df)], as.numeric)
      # Append header
      reg_df <- reg_df %>%
        mutate(
          region = df_hdrs[i]
        )
    }
  }
  
  # Assign gender, type, and load date
  reg_df <- reg_df %>%
    mutate(
      gender = gender,
      type = type,
      div = div,
      load_dt = lubridate::today()
    )
  
  # Return data
  return(reg_df)
}

# URLs
national_urls <- c(
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=1&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=2&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=488&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=489&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=490&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=491&season="
)

regional_urls <- c(
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=510&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=511&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=516&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=517&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=522&season=",
  "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=523&season="
)

# Years
years <- c(2011:2022)

# Divs
divs <- c(
  "D1",
  "D1",
  "D2",
  "D2",
  "D3",
  "D3"
)

# Get historic national rankings
for (i in 1:length(years)) {
  # Iterate over URLs
  for (j in 1:length(national_urls)) {
    # Get gender
    if(j %% 2 == 0) {
      gender <- "F"
    } else {
      gender <- "M"
    }
    
    # Get rankings
    if(exists("nat_df")) {
      temp_df <- getHistoricNationalRankings(url = national_urls[j], gender = gender, season = years[i], div = divs[j])
      if(!is.null(temp_df)) {
        temp_df$season = years[i]
        nat_df <- plyr::rbind.fill(nat_df, temp_df)
      } else {
        print(paste0("Bad data for ", national_urls[j]))
      }
      Sys.sleep(30)
    } else {
      nat_df <- getHistoricNationalRankings(url = national_urls[j], gender = gender, season = years[i], div = divs[j])
      nat_df$season = years[i]
      Sys.sleep(30)
    }
  }
}

# Get historic regional rankings
for (i in 1:length(years)) {
  # Status check
  print(paste0("Getting data for year ", years[i]))
  # Iterate over URLs
  for (j in 1:length(regional_urls)) {
    # Get gender
    if(j %% 2 == 0) {
      gender <- "F"
    } else {
      gender <- "M"
    }
    print(paste0("Getting data for ", regional_urls[j]))
    # Get rankings
    if(exists("regional_df")) {
      temp_df <- getHistoricRegionalRankings(url = regional_urls[j], gender = gender, season = years[i], div = divs[j])
      if(!is.null(temp_df)) {
        temp_df$season = years[i]
        regional_df <- plyr::rbind.fill(regional_df, temp_df) %>%
          funique()
      } else {
        print("Skipping iteration...")
      }
      Sys.sleep(15)
    } else {
      regional_df <- getHistoricRegionalRankings(url = regional_urls[j], gender = gender, season = years[i], div = divs[j])
      regional_df$season = years[i]
      Sys.sleep(15)
    }
  }
}

# Clean up data
nat_df <- nat_df %>%
  select(
    -c(X1, X2)
  )

# regional_df <- regional_df %>%
#   select(
#     -c(X1, X2)
#   )

# Convert to dataframes
nat_df <- as.data.frame(nat_df)
regional_df <- as.data.frame(regional_df)

# Upload to dataframe
# National
dbRemoveTable(pg, "historic_xc_national_rankings")
dbCreateTable(pg, "historic_xc_national_rankings", nat_df)
dbWriteTable(pg, "historic_xc_national_rankings", nat_df, overwrite = TRUE)

# Regional
dbRemoveTable(pg, "historic_xc_regional_rankings")
dbCreateTable(pg, "historic_xc_regional_rankings", regional_df)
dbWriteTable(pg, "historic_xc_regional_rankings", regional_df, overwrite = TRUE)
