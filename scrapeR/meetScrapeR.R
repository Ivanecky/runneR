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

# Wrap it all in a function
meetScrapeR <- function(meetURL) {
  
  # Check URL validity
  if(class(try(meetURL %>%
               GET(., timeout(30)) %>%
               read_html())) == 'try-error') {
    print(paste0("Failed to get data for : ", url))
    return(NA)
  }
  
  print(paste0("Getting data for: ", meetURL))
  
  # Get meet html
  meetHtml <- meetURL %>%
    GET(., timeout(30)) %>%
    read_html()
  
  # Read the meet tables
  meetTbls <- meetHtml %>%
    html_table()
  
  # Get meet name & details
  meetDets <- meetHtml %>%
    html_nodes("h3") %>%
    html_text()
  
  # Extract meet name
  meetName <- str_trim(unlist(strsplit(meetDets[1], "[\n\t]"))[5])
  
  # Extract meet date
  meetDate <- meetHtml %>%
    html_nodes(xpath = "//html/body/div[3]/div/div/div[2]/div[1]/div/div/div/div[1]") %>%
    html_text()
  
  # Extract order of tables
  # Empty vector to hold results
  tblOrder <- vector()
  subSections <- vector()
  
  # Dataframes to hold results
  teams <- as.data.frame(cbind(1.1, "Team", 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, "F", "Test", "Test", "Test"))
  # Rename columns
  names(teams) = c("Place", "Team", "Score", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "Gender", "Subsection", "Meet_Name", "Meet_Date")
  
  inds <- as.data.frame(cbind(1.1, "Name", "Test", "Test", "Test", 1.1, "Test", "Test", "Test", "Test"))
  # Rename columns
  names(inds) = c("Place", "Name", "Year", "Team", "Time", "Score", "Gender", "Subsection", "Meet_Name", "Meet_Date")
  
  # Iterate over remaining values in dets
  for( i in 2:length(meetDets)) {
    # Check line
    tempLine <- tolower(str_trim(unlist(strsplit(meetDets[i], "[\n\t]"))[1]))
    # Check cases 
    if(grepl("women", tempLine)) {
      if(grepl("team", tempLine)) {
        tblOrder <- append(tblOrder, "Women - Team")
        subSections <- append(subSections, tempLine)
      } else {
        tblOrder <- append(tblOrder, "Women - Ind")
        subSections <- append(subSections, tempLine)
      }
    } else {
      if(grepl("team", tempLine)) {
        tblOrder <- append(tblOrder, "Men - Team")
        subSections <- append(subSections, tempLine)
      } else {
        tblOrder <- append(tblOrder, "Men - Ind")
        subSections <- append(subSections, tempLine)
      }
    }
  }
  
  # Iterate over tables
  for (i in 1:length(meetTbls)) {
    # Set temp table
    tempTbl <- as.data.frame(meetTbls[i])
    names(tempTbl) <- toupper(names(tempTbl))
    # Check if team or individual
    if (tblOrder[i] == 'Women - Team') {
      # Subset for team columns
      tempTbl <- tempTbl %>%
        select(PL, TEAM, SCORE, X1, X2, X3, X4, X5, X6, X7) %>%
        rename(
          Place = PL,
          Team = TEAM,
          Score = SCORE,
          R1 = X1,
          R2 = X2,
          R3 = X3,
          R4 = X4,
          R5 = X5,
          R6 = X6,
          R7 = X7
        ) %>%
        mutate(
          Gender = 'F',
          Subsection = subSections[i],
          Meet_Name = meetName,
          Meet_Date = meetDate
        )
      
      # Append to teams
      teams <- rbind(teams, tempTbl)
      
    } else if (tblOrder[i] == 'Men - Team') {
      # Subset for team columns
      tempTbl <- tempTbl %>%
        select(PL, TEAM, SCORE, X1, X2, X3, X4, X5, X6, X7) %>%
        rename(
          Place = PL,
          Team = TEAM,
          Score = SCORE,
          R1 = X1,
          R2 = X2,
          R3 = X3,
          R4 = X4,
          R5 = X5,
          R6 = X6,
          R7 = X7
        ) %>%
        mutate(
          Gender = 'M',
          Subsection = subSections[i],
          Meet_Name = meetName,
          Meet_Date = meetDate
        )
      
      # Append to teams
      teams <- rbind(teams, tempTbl)
      
    } else if (tblOrder[i] == 'Women - Ind') {
      # Subset for ind columns
      tempTbl <- tempTbl %>%
        select(PL, NAME, YEAR, TEAM, TIME, SCORE) %>%
        rename(
          Place = PL,
          Name = NAME,
          Year = YEAR,
          Team = TEAM,
          Time = TIME,
          Score = SCORE
        ) %>%
        mutate(
          Gender = 'F',
          Subsection = subSections[i],
          Meet_Name = meetName,
          Meet_Date = meetDate
        )
      
      # Append to ind
      inds <- rbind(inds, tempTbl)
      
    } else if (tblOrder[i] == 'Men - Ind') {
      # Subset for ind columns
      tempTbl <- tempTbl %>%
        select(PL, NAME, YEAR, TEAM, TIME, SCORE) %>%
        rename(
          Place = PL,
          Name = NAME,
          Year = YEAR,
          Team = TEAM,
          Time = TIME,
          Score = SCORE
        ) %>%
        mutate(
          Gender = 'M',
          Subsection = subSections[i],
          Meet_Name = meetName,
          Meet_Date = meetDate
        )
      
      # Append to ind
      inds <- rbind(inds, tempTbl)
      
    } else {
      # If none, skip to next table (catch statement)
      next
    }
  }
  
  # Upload meets to AWS
  # Connect to AWS
  aws <- dbConnect(
    RPostgres::Postgres(),
    host = aws.yml$host,
    user = aws.yml$user,
    password = aws.yml$password,
    port = aws.yml$port
  )
  
  # Filter out extra rows
  inds <- inds %>%
    filter(Team != 'Test')
  
  teams <- teams %>%
    filter(Team != 'Team')
  
  # Write tables
  dbWriteTable(aws, "xc_team_results", teams, append = TRUE)
  dbWriteTable(aws, "xc_ind_results", inds, append = TRUE)
}

