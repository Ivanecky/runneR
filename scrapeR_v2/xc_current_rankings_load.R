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
pg.yml <- read_yaml("/Users/samivanecky/git/runneR/postgres.yaml")

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
getCurrentNationalRankings <- function(url, gender, div, type = "national") {
  # Mens data
  tbls <- url %>%
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

getCurrentRegionalRankings <- function(url, gender, div, type = "regional") {
  # Read tbls
  df_tbls <- url %>%
    read_html() %>%
    html_table()
  
  # Number of regions differs on division, set tbls by division
  n_tbls <- case_when(
    div == "D1" ~ 2,
    div == "D2" ~ 2,
    div == "D3" ~ 2,
    T ~ 0
  )
  
  # Drop last tbls
  df_tbls <- df_tbls[1:(length(df_tbls) - n_tbls)]
  
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

# Current National URLs
# D1
m_d1_n_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=1"
w_d1_n_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=2"
m_d1_r_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=510&season=2022"
w_d1_r_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=511&season=2022"
# D2
m_d2_n_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=489&season=2022"
w_d2_n_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=488&season=2022"
m_d2_r_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=516&season=2022"
w_d2_r_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=517&season=2022"
# D3
m_d3_n_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=490&season=2022"
w_d3_n_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=491&season=2022"
m_d3_r_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=522&season=2022"
w_d3_r_url <- "http://www.ustfccca.org/team-rankings-polls-central/polls-and-rankings-week-by-week?pritype=523&season=2022"

# Get current rankings
# D1
m_d1_nat <- getCurrentNationalRankings(m_d1_n_url, "M", "D1", "national")
w_d1_nat <- getCurrentNationalRankings(w_d1_n_url, "F", "D1", "national")
m_d1_reg <- getCurrentRegionalRankings(m_d1_r_url, "M", "D1", "regional")
w_d1_reg <- getCurrentRegionalRankings(w_d1_r_url, "F", "D1", "regional")
# D2
m_d2_nat <- getCurrentNationalRankings(m_d2_n_url, "M", "D2", "national")
w_d2_nat <- getCurrentNationalRankings(w_d2_n_url, "F", "D2", "national")
m_d2_reg <- getCurrentRegionalRankings(m_d2_r_url, "M", "D2", "regional")
w_d2_reg <- getCurrentRegionalRankings(w_d2_r_url, "F", "D2", "regional")
# D3
m_d3_nat <- getCurrentNationalRankings(m_d3_n_url, "M", "D3", "national")
w_d3_nat <- getCurrentNationalRankings(w_d3_n_url, "F", "D3", "national")
m_d3_reg <- getCurrentRegionalRankings(m_d3_r_url, "M", "D3", "regional")
w_d3_reg <- getCurrentRegionalRankings(w_d3_r_url, "F", "D3", "regional")

# Combine dataframes
national <- plyr::rbind.fill(m_d1_nat, m_d2_nat, m_d3_nat, w_d1_nat, w_d2_nat, w_d3_nat)
regional <- plyr::rbind.fill(m_d1_reg, m_d2_reg, m_d3_reg, w_d1_reg, w_d2_reg, w_d3_reg)

# Upload to dataframes
# dbCreateTable(pg, "xc_nat_rank_current", national)
dbWriteTable(pg, "xc_nat_rank_current", national, overwrite = TRUE)
# dbCreateTable(pg, "xc_reg_rank_current", regional)
dbWriteTable(pg, "xc_reg_rank_current", regional, overwrite = TRUE)
