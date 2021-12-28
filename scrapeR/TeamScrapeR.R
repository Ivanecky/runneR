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
source("Scraping_Fxns.R")
source("ResultsQuery.R")

# Test URL
url <- "https://www.tfrrs.org/results/xc/18879/Joe_Piane_Invitational#123583"

# Read tables from page
tbls <- read_html(url) %>%
  html_table()

# Create empty dataframes to hold data
teams <- as.data.frame(cbind("PL", "Team", "Score", "R1", "R2", "R3", "R4", "R5", "R6", "R7"))
names(teams) <- c("PL", "Team", "Score", "R1", "R2", "R3", "R4", "R5", "R6", "R7")
runners <- as.data.frame(cbind("PL", "Name", "Team", "Year", "Time"))
names(runners) <- c("PL", "Name", "Team", "Year", "Time")

# Iterate over tables
for (i in 1:length(tbls)) {
  # Convert to df
  temp_df <- as.data.frame(tbls[i])
  
  # Check number of columns
  # If more than 7, team results
  if (ncol(temp_df) > 7) {
    # Subset data
    temp_df <- temp_df %>%
      select(PL, Team, Score, X1, X2, X3, X4, X5, X6, X7)
    
    # Rename for binding
    names(temp_df) <- c("PL", "Team", "Score", "R1", "R2", "R3", "R4", "R5", "R6", "R7")
    
    # Bind
    teams <- rbind(teams, temp_df)
    
  } else {
    # Subset data
    temp_df <- temp_df %>%
      select(PL, NAME, TEAM, YEAR, TIME)
    
    # Rename for binding
    names(temp_df) <- c("PL", "Name", "Team", "Year", "Time")
    
    # Bind
    runners <- rbind(runners, temp_df)
  }
}

