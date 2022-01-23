# Functions used in scraping data off of the TFFRS web page

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
library(foreach)
library(doParallel)

# DYNAMICALLY SCRAPE TFRRS LINKS
## getURLs
# Function for results URLs
getResultsURLs <- function(url) {
  # Read in the links on the webpage
  wp = read_html(url) %>%
    html_nodes(xpath = "//td/div/a") %>%
    html_attr("href")
  
  # Manipulate strings
  for ( i  in 1:length(wp) )
  {
    temp = wp[i]
    #temp = substring(temp, 3)
    temp = paste0("https:", temp)
    temp = substr(temp, 1, nchar(temp)-3)
    wp[i] = temp
  }
  
  # Grab only strings for athletes
  wp = wp[grepl("athletes", wp)]
  
  # Return webpage results
  return(wp)
  
}

# Function for Performance Lists
getPerfListURLs <- function(url) {
  # Read in the links on the webpage
  wp <- read_html(url) %>%
    html_nodes(xpath = "//td/a") %>%
    html_attr("href")
  
  # Manipulate strings
  for ( i  in 1:length(wp) )
  {
    temp = wp[i]
    #temp = substring(temp, 3)
    temp = paste0("https:", temp)
    temp = substr(temp, 1, nchar(temp)-3)
    wp[i] = temp
  }
  
  # Grab only strings for athletes
  wp = wp[grepl("athletes", wp)]
  
  # Return webpage results
  return(wp)
  
}

# Function to handle times
handleTimes <- function(mark) {
  # Convert mark to character
  mark <- as.character(mark)
  # Check if mark has no colon (sprint time)
  if (grepl('\"|m', mark, ignore.case = TRUE)) {
    return(mark)
  } else if (!(grepl(":", mark))) {
    # Handle the parentheses for ties, etc
    mark <- gsub("\\(.*","", mark)
    mark <- gsub("@.*","", mark)
    # Convert to numeric
    # mark <- as.numeric(mark)
    return(mark)
  } else {
    # Split the tenths/hundreths off
    # split_mark = unlist(strsplit(mark, "[.]"))
    # Split into minutes & seconds
    min_sec <- unlist(strsplit(mark, ":"))
    # Calculate seconds from minutes and add seconds
    total_time <- as.numeric(min_sec[1])*60 + as.numeric(min_sec[2])
    # Return time 
    total_time <- as.character(total_time)
    return(total_time)
  }
}

## Function to select user agent 
randUsrAgnt <- function() {
  # Create list of user agents
  usrAgents <- c(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1",
    "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36"
  )
  
  # Return a random one
  return(usrAgents[sample(1:length(usrAgents), 1)])
  
}

## runnerScrape Function
runnerScrape <- function(url){
  
    # Clean up URL from potential whitespace
    url <- gsub("[[:space:]]", "", url)
  
    # Error checking. Validate URL
    if(class(try(url %>%
                 GET(., timeout(30), user_agent(randUsrAgnt())) %>%
                 read_html())) == 'try-error') {
      print(paste0("Failed to get data for : ", url))
      return(NA)
    } else {
      
      # Get page HTML
      html <-  url %>%
        GET(., timeout(30), user_agent(randUsrAgnt())) %>%
        read_html()
    
      # Get the name of the runner off of TFRRS HTML
      runner_name <- html %>%
        html_nodes("h3") %>%
        html_text()
      
      runner_name <- unlist(strsplit(runner_name[1], "[\n]"))[1]
      
      # Extract team name
      team_name <- html %>%
        html_node(xpath = "/html/body/form/div/div/div/div[1]/a[3]/h3") %>%
        html_text()
      
      # Strip new line character
      team_name <- gsub('\\n', '', team_name)
      
      # Pull gender out via team link
      team_link <- html %>%
        html_node(xpath = "/html/body/form/div/div/div/div[1]/a[3]") %>%
        html_attr("href")
      
      # Navigate to team link and extract division
      # Convert team page link
      team_link <- paste0("https:", team_link)
      team_link <- paste0(substr(team_link, 1, nchar(team_link)-3), "tml")
      
      # Get text info for division
      team_page <- team_link %>%
        GET(., timeout(30)) %>%
        read_html() %>%
        html_node(xpath = "/html/body/form/div/div/div/div/div/div/span") %>%
        html_text()
      
      # Assign division
      team_division <- case_when(
        grepl("NAIA", team_page) ~ "NAIA",
        grepl("NJCAA", team_page) ~ "NJCAA",
        grepl("DIII", team_page) ~ "D3",
        grepl("DII", team_page) ~ "D2",
        grepl("DI", team_page) ~ "D1",
        T ~ "OTHER"
      )
      
      # Check if mens or womens team
      gender <- case_when(
        grepl("college_m", team_link) ~ 'M',
        grepl("college_f", team_link) ~ 'F',
        T ~ 'Other/NA'
      )
      
      # Print statement
      print(paste0("Getting data for: ", stringr::str_to_title(runner_name)))
      
      runner <- html %>%
        html_nodes(xpath = '/html/body/form/div/div/div/div[3]/div/div/div[1]') %>%
        html_nodes("tr")
      
      keep_nodes <- c()
      
      for(i in 1:length(runner))
      {
        #if(grepl("<th", test[i]) == FALSE)
        {
          keep_nodes <- c(keep_nodes, html_text(runner[i]))
        }
      }
      
      for(i in 1:length(keep_nodes))
      {
        keep_nodes[i] = str_squish(gsub("[\t\n]", " ", keep_nodes[i]))
      }
      
      # Loop over to get stuff
      # Vectors
      years <- c()
      events <- c()
      marks <- c()
      places <- c()
      race_names <- c()
      dates <- c()
      prelims <- c()
      
      for(i in 1:length(keep_nodes))
      {
        # Check for a year (date)
        if(grepl("2005|2006|2007|2008|2009|2010|2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021|2022|2023|2024", keep_nodes[i]))
        {
          # Convert to string
          node_str <- as.character(keep_nodes[i])
          
          # Extract year
          year <- case_when(
            grepl('2005', node_str) ~ '2005',
            grepl('2006', node_str) ~ '2006',
            grepl('2007', node_str) ~ '2007',
            grepl('2008', node_str) ~ '2008',
            grepl('2009', node_str) ~ '2009',
            grepl('2010', node_str) ~ '2010',
            grepl('2011', node_str) ~ '2011',
            grepl('2012', node_str) ~ '2012',
            grepl('2013', node_str) ~ '2013',
            grepl('2014', node_str) ~ '2014',
            grepl('2015', node_str) ~ '2015',
            grepl('2016', node_str) ~ '2016',
            grepl('2017', node_str) ~ '2017',
            grepl('2018', node_str) ~ '2018',
            grepl('2019', node_str) ~ '2019',
            grepl('2020', node_str) ~ '2020',
            grepl('2021', node_str) ~ '2021',
            grepl('2022', node_str) ~ '2022',
            grepl('2023', node_str) ~ '2024',
            T ~ 'OTHER'
          )
          
          # Extract month
          temp_mo <- case_when(
            grepl("Jan", node_str) ~ '1',
            grepl('Feb', node_str) ~ '2',
            grepl('Mar', node_str) ~ '3',
            grepl("Apr", node_str) ~ '4',
            grepl('May', node_str) ~ '5',
            grepl('Jun', node_str) ~ '6',
            grepl('Jul', node_str) ~ '7',
            grepl('Aug', node_str) ~ '8',
            grepl('Sep', node_str) ~ '9',
            grepl('Oct', node_str) ~ '10',
            grepl('Nov', node_str) ~ '11',
            grepl('Dec', node_str) ~ '12',
            T ~ '0'
          )
          
          # Split up string
          node_split <- case_when(
            grepl("Jan", node_str) ~ str_split(node_str, 'Jan'),
            grepl('Feb', node_str) ~ str_split(node_str, 'Feb'),
            grepl('Mar', node_str) ~ str_split(node_str, 'Mar'),
            grepl("Apr", node_str) ~ str_split(node_str, 'Apr'),
            grepl('May', node_str) ~ str_split(node_str, 'May'),
            grepl('Jun', node_str) ~ str_split(node_str, 'Jun'),
            grepl('Jul', node_str) ~ str_split(node_str, 'Jul'),
            grepl('Aug', node_str) ~ str_split(node_str, 'Aug'),
            grepl('Sep', node_str) ~ str_split(node_str, 'Sep'),
            grepl('Oct', node_str) ~ str_split(node_str, 'Oct'),
            grepl('Nov', node_str) ~ str_split(node_str, 'Nov'),
            T ~ str_split(node_str, 'Dec')
          )
          
          # Unlist
          node_split <- unlist(node_split)
          
          # Extract meet name
          race_name <- str_trim(node_split[1])
          
          # Get day (take first day if it's a two day meet)
          race_day <- case_when(
            substr(str_trim(node_split[2]), 2, 2) == '-' ~ substr(str_trim(node_split[2]), 1, 1),
            T ~ substr(str_trim(node_split[2]), 1, 2)
          )
          
          # Create date using day/mo/yr
          temp_race_date <- paste0(year, '-', temp_mo, '-', race_day)
          
          # Create a flag for XC
          xc_flag <- case_when(
            grepl("xc|cross country", tolower(node_str)) ~ TRUE,
            T ~ FALSE
          )
          
          # Now we extract results
          for (j in (i+1):length(keep_nodes))
          {
            if(grepl("2005|2006|2007|2008|2009|2010|2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021|2022", keep_nodes[j]))
            {
              # If so, break out
              break
            }
            # Check if a prelim
            is_prelim <- case_when(
              grepl("\\(P\\)", keep_nodes[j]) ~ TRUE,
              T ~ FALSE
            )
            # Split the next string into event, time and place
            result_split <- unlist(str_split(keep_nodes[j], " "))
            # Check to see if event is field event
            # Handle sprints
            if(result_split[1] %in% c("60", "60H", "100", "200", "100H", "110H", "400H", "110SH", "300", "400"))
            {
              # Handle sprints
              event <- result_split[1]
              
              # Get place (need code to handle sprinters)
              place <- case_when(
                length(result_split) == 5 ~ result_split[4],
                T ~ result_split[3]
              )
              # Get time
              mark <- result_split[2]
              
            } else if (result_split[1] %in% c("HJ","LJ","TJ","PV","SP","DT","HT","JT","Hep","Dec","Pent")) {
              # Handle field events
              # Indicate if result is XC or not
              event <- result_split[1]

              # Get place (need code to handle sprinters)
              place <- case_when(
                length(result_split) == 7 ~ result_split[6],
                length(result_split) == 6 ~ result_split[5],
                T ~ result_split[5]
              )
              # Get distance (height, throw) using metric distance (easier)
              mark <- result_split[2]
              
            } else if(result_split[1] %in% c("4x400", "4x100", "4x200", "4x800", "DMR", "SMR", "800SMR")) { 
              # Handle relays
              event <- result_split[1]

              # Get place (need code to handle sprinters)
              place <- result_split[3]
              
              # Get time
              mark <- result_split[2]
              
            } else { # Handle distance events
              # Indicate if result is XC or not
              event <- case_when(
                xc_flag ~ paste0(result_split[1], " XC"),
                T ~ result_split[1]
              )
              # Get place (need code to handle sprinters)
              place <- case_when(
                length(result_split) == 5 ~ result_split[4],
                T ~ result_split[3]
              )
              # Get time
              mark <- result_split[2]
            }
            # Get the pieces
            years <- append(years, year)
            events <- append(events, event)
            marks <- append(marks, mark)
            places <- append(places, place)
            race_names <- append(race_names, race_name)
            dates <- append(dates, temp_race_date)
            prelims <- append(prelims, is_prelim)
          }
        }
      }
      
      # Check if vectors are null
      if (is.null(years) | is.null(events) | is.null(marks) | is.null(places) | is.null(race_names) | is.null(dates) | is.null(prelims)) {
        # Print error message
        print(paste0("Error getting data for: ", runner_name))
        # Set to empty, default values
        athlete <- as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division", FALSE, "1"))
        # Rename columns
        names(athlete) = c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION", "IS_FIELD", "MARK_TIME")
      } else {
        # Create a data frame
        athlete <- as.data.frame(cbind(years, events, marks, places, race_names, dates, prelims))
        names(athlete) <- c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM")
      }
      
      # Remove results where athlete competed in field event
      athlete <- athlete %>%
        mutate(
          EVENT = as.character(EVENT),
          MEET_NAME = as.character(MEET_NAME),
          MEET_DATE = as.character(MEET_DATE)
        ) %>%
        mutate(
          EVENT = gsub("[^\x01-\x7F]", "", EVENT),
          MEET_DATE = lubridate::ymd(gsub(",", "", MEET_DATE))
        )
  
      # Make sure athlete has rows (sprinters get removed)
      if ((nrow(athlete) > 0)) {
        # Clean up and group to one row per event
        athlete <- athlete %>%
          mutate(
            EVENT = case_when(
              grepl("5000|5K|5k", EVENT) & (lubridate::month(lubridate::ymd(MEET_DATE)) %in% c(8, 9, 10, 11)) ~ "5K XC",
              grepl("5000|5K|5k", EVENT) & !(grepl("XC|xc|Xc", EVENT)) ~ "5000m",
              grepl("3000|3K|3k", EVENT) & !grepl("3000S|3000mS|3000SC|.3", EVENT) ~ "3000m",
              grepl("mile|Mile|MILE", EVENT) ~ "Mile",
              grepl("4K|4.1K", EVENT) ~ "4K XC",
              grepl("6K|6k|6000|6.", EVENT) & (lubridate::month(lubridate::ymd(MEET_DATE)) %in% c(8, 9, 10, 11)) ~ "6K XC",
              grepl("800|800m", EVENT) & !grepl("8000m|8000", EVENT) & !grepl("SMR", EVENT) ~ "800m",
              grepl("8k|8K|8000m", EVENT) & !grepl("\\.", EVENT) ~ "8K XC",
              grepl("10,000|10K|10k|10000", EVENT) & grepl("XC|xc|Xc", EVENT) ~ "10K XC",
              grepl("10,000|10K|10k|10000", EVENT) & (lubridate::month(lubridate::ymd(MEET_DATE)) %in% c(10, 11)) ~ "10K XC",
              grepl("10,000|10K|10k|10000", EVENT) & !(grepl("XC|xc|Xc", EVENT)) ~ "10K",
              grepl("1500|1500m", EVENT) & !grepl("4x", EVENT) ~ "1500m",
              grepl("3000S|3000s|3000SC|3000sc|3000mS", EVENT) ~ "3000S",
              grepl("HJ|LJ|TJ|PV|SP|DT|HT|JT|Hep|Dec|Pent", EVENT) ~ EVENT,
              grepl("4x400|4x100|4x200|4x800|DMR|SMR|800SMR", EVENT) ~ EVENT,
              grepl("60", EVENT) & !grepl("6K|6000m", EVENT) ~ "60m",
              grepl("60H", EVENT, ignore.case = TRUE) ~ "60H",
              grepl("60SH", EVENT, ignore.case = TRUE) ~ "60SH",
              grepl("100", EVENT) & !grepl("1000m|1K|1000", EVENT, ignore.case = TRUE) ~ "100m",
              grepl("100H", EVENT, ignore.case = TRUE) ~ "100H",
              grepl("100SH", EVENT, ignore.case = TRUE) ~ "100SH",
              grepl("110SH", EVENT, ignore.case = TRUE) ~ "110SH",
              grepl("110H", EVENT, ignore.case = TRUE) ~ "110H",
              grepl("400H", EVENT, ignore.case = TRUE) ~ "400H",
              grepl("400", EVENT) & !grepl("4x400|4000m|4000", EVENT, ignore.case = TRUE) ~ "400m",
              grepl("1000", EVENT) & !grepl("10000|10,000|10K", EVENT, ignore.case = TRUE) ~ "1000m",
              T ~ "OTHER"
            ),
            PLACE = as.character(PLACE)
          ) %>%
          # filter(grepl("th|TH|st|ST|nd|ND|rd|RD", PLACE, ignore.case = TRUE)) %>%
          mutate(
            PLACE = as.numeric(gsub("th|TH|st|ST|nd|ND|rd|RD", "", PLACE))
          ) %>%
          # filter(!is.na(PLACE)) %>%
          mutate(
            MARK = as.character(MARK)
          )
        
        # Check to see if data was nullified
        if (nrow(athlete) == 0) {
          # Set to empty, default values
          athlete <- as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division", FALSE, "1"))
          # Rename columns
          names(athlete) = c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION", "IS_FIELD", "MARK_TIME")
          # Return
          return(athlete)
        } 
        
        # Apply function to convert times to numbers
        athlete$MARK <- marks # sapply(athlete$TIME, handleTimes)
        athlete$NAME <- runner_name
        athlete$GENDER <- gender
        athlete$TEAM <- team_name
        athlete$DIVISION <- team_division
        
        # Apply the handle times function to the mark column on a selective basis
        athlete <- athlete %>%
          mutate(
            IS_FIELD = case_when(
              EVENT %in% c("HJ","LJ","TJ","PV","SP","DT","HT","JT","Hep","Dec","Pent") ~ TRUE,
              T ~ FALSE
            )
          ) %>%
          rowwise() %>%
          mutate(
            MARK_TIME = handleTimes(MARK)
          )
        
      } else if (nrow(athlete) == 0) {
        # Set to empty, default values
        athlete <- as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division", FALSE, "1"))
        # Rename columns
        names(athlete) = c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION", "IS_FIELD", "MARK_TIME")
      } else {
        # Set to empty, default values
        athlete <- as.data.frame(cbind("year", "event", 1.1, 1.1, "meet", "meet date", TRUE, "name", "gender", "team_name", "team_division", FALSE, "1"))
        # Rename columns
        names(athlete) = c("YEAR", "EVENT", "MARK", "PLACE", "MEET_NAME", "MEET_DATE", "PRELIM", "NAME", "GENDER", "TEAM", "DIVISION", "IS_FIELD", "MARK_TIME")
      }
      
      # Final return call
      return(athlete)
    }
}

groupedResults <- function(athleteDF){
  athlete <- athleteDF %>%
    filter(PRELIM == FALSE & !is.na(TIME) & !is.na(PLACE)) %>%
    group_by(NAME, GENDER, TEAM, EVENT) %>%
    summarise(
      AVG_PLACE = round(mean(PLACE, na.rm = T), 2),
      AVG_TIME = round(mean(TIME, na.rm = T), 2),
      PR = min(TIME, na.rm = T),
      WINS = n_distinct(TIME[PLACE == 1]),
      TIMES.RUN = n()
    ) %>%
    mutate(
      WIN_PCT = round(((WINS / TIMES.RUN) * 100), 2)
    )
  
  # Return data
  return(athlete)
}

groupedYearlyResults <- function(athleteDF){
  athlete <- athleteDF %>%
    filter(PRELIM == FALSE) %>%
    group_by(NAME, GENDER, TEAM, DIVISION, EVENT, YEAR) %>%
    summarise(
      AVG_PLACE = round(mean(PLACE, na.rm = T), 2),
      AVG_TIME = round(mean(TIME), 2),
      PR = min(TIME),
      WINS = n_distinct(TIME[PLACE == 1]),
      TIMES.RUN = n()
    ) %>%
    mutate(
      WIN_PCT = round(((WINS / TIMES.RUN) * 100), 2)
    )
  
  # Return data
  return(athlete)
}

## reformatRunners
# Create function to convert times and transpose
reformatRunners = function(df){
  df <- df %>%
    mutate( # Create times that are readable for QC purposes
      AVG_TIME_FORM = paste0(floor(as.numeric(AVG_TIME) / 60), ":", (str_pad(floor(as.numeric(AVG_TIME) %% 60), width = 2, side = "left", pad = "0")))
    ) %>%
    mutate(
      PR_FORM = paste0(floor(as.numeric(PR) / 60), ":", (str_pad(floor(as.numeric(PR) %% 60), width = 2, side = "left", pad = "0")))
    ) %>%
    filter(!(EVENT %in% c("event", "OTHER"))) %>%
    group_by(NAME, GENDER, TEAM) %>%
    summarise( # Transpose data frame
      # 800m
      AVG_PLACE_800 = min(AVG_PLACE[EVENT == '800m']),
      AVG_TIME_800 = min(AVG_TIME[EVENT == '800m']),
      PR_800 = min(PR[EVENT == '800m']),
      WINS_800 = min(WINS[EVENT == '800m']),
      TIMES_RUN_800 = min(TIMES.RUN[EVENT == '800m']),
      WIN_PCT_800 = min(WIN_PCT[EVENT == '800m']),
      AVG_TIME_FORM_800 = min(AVG_TIME_FORM[EVENT == '800m']),
      PR_FORM_800 = min(PR_FORM[EVENT == '800m']),
      # 1500m
      AVG_PLACE_1500 = min(AVG_PLACE[EVENT == '1500m']),
      AVG_TIME_1500 = min(AVG_TIME[EVENT == '1500m']),
      PR_1500 = min(PR[EVENT == '1500m']),
      WINS_1500 = min(WINS[EVENT == '1500m']),
      TIMES_RUN_1500 = min(TIMES.RUN[EVENT == '1500m']),
      WIN_PCT_1500 = min(WIN_PCT[EVENT == '1500m']),
      AVG_TIME_FORM_1500 = min(AVG_TIME_FORM[EVENT == '1500m']),
      PR_FORM_1500 = min(PR_FORM[EVENT == '1500m']),
      # Mile
      AVG_PLACE_MILE = min(AVG_PLACE[EVENT == 'Mile']),
      AVG_TIME_MILE = min(AVG_TIME[EVENT == 'Mile']),
      PR_MILE = min(PR[EVENT == 'Mile']),
      WINS_MILE = min(WINS[EVENT == 'Mile']),
      TIMES_RUN_MILE = min(TIMES.RUN[EVENT == 'Mile']),
      WIN_PCT_MILE = min(WIN_PCT[EVENT == 'Mile']),
      AVG_TIME_FORM_MILE = min(AVG_TIME_FORM[EVENT == 'Mile']),
      PR_FORM_MILE = min(PR_FORM[EVENT == 'Mile']),
      # 3000m
      AVG_PLACE_3000 = min(AVG_PLACE[EVENT == '3000m']),
      AVG_TIME_3000 = min(AVG_TIME[EVENT == '3000m']),
      PR_3000 = min(PR[EVENT == '3000m']),
      WINS_3000 = min(WINS[EVENT == '3000m']),
      TIMES_RUN_3000 = min(TIMES.RUN[EVENT == '3000m']),
      WIN_PCT_3000 = min(WIN_PCT[EVENT == '3000m']),
      AVG_TIME_FORM_3000 = min(AVG_TIME_FORM[EVENT == '3000m']),
      PR_FORM_3000 = min(PR_FORM[EVENT == '3000m']),
      # 3000mSC
      AVG_PLACE_3000S = min(AVG_PLACE[EVENT == '3000S']),
      AVG_TIME_3000S = min(AVG_TIME[EVENT == '3000S']),
      PR_3000S = min(PR[EVENT == '3000S']),
      WINS_3000S = min(WINS[EVENT == '3000S']),
      TIMES_RUN_3000S = min(TIMES.RUN[EVENT == '3000S']),
      WIN_PCT_3000S = min(WIN_PCT[EVENT == '3000S']),
      AVG_TIME_FORM_3000S = min(AVG_TIME_FORM[EVENT == '3000S']),
      PR_FORM_3000S = min(PR_FORM[EVENT == '3000S']),
      # 5000m
      AVG_PLACE_5000 = min(AVG_PLACE[EVENT == '5000m']),
      AVG_TIME_5000 = min(AVG_TIME[EVENT == '5000m']),
      PR_5000 = min(PR[EVENT == '5000m']),
      WINS_5000 = min(WINS[EVENT == '5000m']),
      TIMES_RUN_5000 = min(TIMES.RUN[EVENT == '5000m']),
      WIN_PCT_5000 = min(WIN_PCT[EVENT == '5000m']),
      AVG_TIME_FORM_5000 = min(AVG_TIME_FORM[EVENT == '5000m']),
      PR_FORM_5000 = min(PR_FORM[EVENT == '5000m']),
      # 6K XC
      AVG_PLACE_6KXC = min(AVG_PLACE[EVENT == '6K XC']),
      AVG_TIME_6KXC = min(AVG_TIME[EVENT == '6K XC']),
      PR_6KXC = min(PR[EVENT == '6K XC']),
      WINS_6KXC = min(WINS[EVENT == '6K XC']),
      TIMES_RUN_6KXC = min(TIMES.RUN[EVENT == '6K XC']),
      WIN_PCT_6KXC = min(WIN_PCT[EVENT == '6K XC']),
      AVG_TIME_FORM_6KXC = min(AVG_TIME_FORM[EVENT == '6K XC']),
      PR_FORM_6KXC = min(PR_FORM[EVENT == '6K XC']),
      # 8K XC
      AVG_PLACE_8KXC = min(AVG_PLACE[EVENT == '8K XC']),
      AVG_TIME_8KXC = min(AVG_TIME[EVENT == '8K XC']),
      PR_8KXC = min(PR[EVENT == '8K XC']),
      WINS_8KXC = min(WINS[EVENT == '8K XC']),
      TIMES_RUN_8KXC = min(TIMES.RUN[EVENT == '8K XC']),
      WIN_PCT_8KXC = min(WIN_PCT[EVENT == '8K XC']),
      AVG_TIME_FORM_8KXC = min(AVG_TIME_FORM[EVENT == '8K XC']),
      PR_FORM_8KXC = min(PR_FORM[EVENT == '8K XC']),
      # 10K
      AVG_PLACE_10K = min(AVG_PLACE[EVENT == '10K']),
      AVG_TIME_10K = min(AVG_TIME[EVENT == '10K']),
      PR_10K = min(PR[EVENT == '10K']),
      WINS_10K = min(WINS[EVENT == '10K']),
      TIMES_RUN_10K = min(TIMES.RUN[EVENT == '10K']),
      WIN_PCT_10K = min(WIN_PCT[EVENT == '10K']),
      AVG_TIME_FORM_10K = min(AVG_TIME_FORM[EVENT == '10K']),
      PR_FORM_10K = min(PR_FORM[EVENT == '10K']),
    ) %>%
    mutate_if(is.numeric, list(~na_if(., Inf))) # Replace Inf values created by runners not competing in an event
  
  # Return data frame
  return(df)
}

reformatYearlyRunners = function(df){
  df <- df %>%
    mutate( # Create times that are readable for QC purposes
      AVG_TIME_FORM = paste0(floor(as.numeric(AVG_TIME) / 60), ":", (str_pad(floor(as.numeric(AVG_TIME) %% 60), width = 2, side = "left", pad = "0")))
    ) %>%
    mutate(
      PR_FORM = paste0(floor(as.numeric(PR) / 60), ":", (str_pad(floor(as.numeric(PR) %% 60), width = 2, side = "left", pad = "0")))
    ) %>%
    filter(!(EVENT %in% c("event", "OTHER"))) %>%
    group_by(NAME, GENDER, TEAM, YEAR) %>%
    summarise( # Transpose data frame
      # 800m
      AVG_PLACE_800 = min(AVG_PLACE[EVENT == '800m']),
      AVG_TIME_800 = min(AVG_TIME[EVENT == '800m']),
      PR_800 = min(PR[EVENT == '800m']),
      WINS_800 = min(WINS[EVENT == '800m']),
      TIMES_RUN_800 = min(TIMES.RUN[EVENT == '800m']),
      WIN_PCT_800 = min(WIN_PCT[EVENT == '800m']),
      AVG_TIME_FORM_800 = min(AVG_TIME_FORM[EVENT == '800m']),
      PR_FORM_800 = min(PR_FORM[EVENT == '800m']),
      # 1500m
      AVG_PLACE_1500 = min(AVG_PLACE[EVENT == '1500m']),
      AVG_TIME_1500 = min(AVG_TIME[EVENT == '1500m']),
      PR_1500 = min(PR[EVENT == '1500m']),
      WINS_1500 = min(WINS[EVENT == '1500m']),
      TIMES_RUN_1500 = min(TIMES.RUN[EVENT == '1500m']),
      WIN_PCT_1500 = min(WIN_PCT[EVENT == '1500m']),
      AVG_TIME_FORM_1500 = min(AVG_TIME_FORM[EVENT == '1500m']),
      PR_FORM_1500 = min(PR_FORM[EVENT == '1500m']),
      # Mile
      AVG_PLACE_MILE = min(AVG_PLACE[EVENT == 'Mile']),
      AVG_TIME_MILE = min(AVG_TIME[EVENT == 'Mile']),
      PR_MILE = min(PR[EVENT == 'Mile']),
      WINS_MILE = min(WINS[EVENT == 'Mile']),
      TIMES_RUN_MILE = min(TIMES.RUN[EVENT == 'Mile']),
      WIN_PCT_MILE = min(WIN_PCT[EVENT == 'Mile']),
      AVG_TIME_FORM_MILE = min(AVG_TIME_FORM[EVENT == 'Mile']),
      PR_FORM_MILE = min(PR_FORM[EVENT == 'Mile']),
      # 3000m
      AVG_PLACE_3000 = min(AVG_PLACE[EVENT == '3000m']),
      AVG_TIME_3000 = min(AVG_TIME[EVENT == '3000m']),
      PR_3000 = min(PR[EVENT == '3000m']),
      WINS_3000 = min(WINS[EVENT == '3000m']),
      TIMES_RUN_3000 = min(TIMES.RUN[EVENT == '3000m']),
      WIN_PCT_3000 = min(WIN_PCT[EVENT == '3000m']),
      AVG_TIME_FORM_3000 = min(AVG_TIME_FORM[EVENT == '3000m']),
      PR_FORM_3000 = min(PR_FORM[EVENT == '3000m']),
      # 3000mSC
      AVG_PLACE_3000S = min(AVG_PLACE[EVENT == '3000S']),
      AVG_TIME_3000S = min(AVG_TIME[EVENT == '3000S']),
      PR_3000S = min(PR[EVENT == '3000S']),
      WINS_3000S = min(WINS[EVENT == '3000S']),
      TIMES_RUN_3000S = min(TIMES.RUN[EVENT == '3000S']),
      WIN_PCT_3000S = min(WIN_PCT[EVENT == '3000S']),
      AVG_TIME_FORM_3000S = min(AVG_TIME_FORM[EVENT == '3000S']),
      PR_FORM_3000S = min(PR_FORM[EVENT == '3000S']),
      # 5000m
      AVG_PLACE_5000 = min(AVG_PLACE[EVENT == '5000m']),
      AVG_TIME_5000 = min(AVG_TIME[EVENT == '5000m']),
      PR_5000 = min(PR[EVENT == '5000m']),
      WINS_5000 = min(WINS[EVENT == '5000m']),
      TIMES_RUN_5000 = min(TIMES.RUN[EVENT == '5000m']),
      WIN_PCT_5000 = min(WIN_PCT[EVENT == '5000m']),
      AVG_TIME_FORM_5000 = min(AVG_TIME_FORM[EVENT == '5000m']),
      PR_FORM_5000 = min(PR_FORM[EVENT == '5000m']),
      # 6K XC
      AVG_PLACE_6KXC = min(AVG_PLACE[EVENT == '6K XC']),
      AVG_TIME_6KXC = min(AVG_TIME[EVENT == '6K XC']),
      PR_6KXC = min(PR[EVENT == '6K XC']),
      WINS_6KXC = min(WINS[EVENT == '6K XC']),
      TIMES_RUN_6KXC = min(TIMES.RUN[EVENT == '6K XC']),
      WIN_PCT_6KXC = min(WIN_PCT[EVENT == '6K XC']),
      AVG_TIME_FORM_6KXC = min(AVG_TIME_FORM[EVENT == '6K XC']),
      PR_FORM_6KXC = min(PR_FORM[EVENT == '6K XC']),
      # 8K XC
      AVG_PLACE_8KXC = min(AVG_PLACE[EVENT == '8K XC']),
      AVG_TIME_8KXC = min(AVG_TIME[EVENT == '8K XC']),
      PR_8KXC = min(PR[EVENT == '8K XC']),
      WINS_8KXC = min(WINS[EVENT == '8K XC']),
      TIMES_RUN_8KXC = min(TIMES.RUN[EVENT == '8K XC']),
      WIN_PCT_8KXC = min(WIN_PCT[EVENT == '8K XC']),
      AVG_TIME_FORM_8KXC = min(AVG_TIME_FORM[EVENT == '8K XC']),
      PR_FORM_8KXC = min(PR_FORM[EVENT == '8K XC']),
      # 10K
      AVG_PLACE_10K = min(AVG_PLACE[EVENT == '10K']),
      AVG_TIME_10K = min(AVG_TIME[EVENT == '10K']),
      PR_10K = min(PR[EVENT == '10K']),
      WINS_10K = min(WINS[EVENT == '10K']),
      TIMES_RUN_10K = min(TIMES.RUN[EVENT == '10K']),
      WIN_PCT_10K = min(WIN_PCT[EVENT == '10K']),
      AVG_TIME_FORM_10K = min(AVG_TIME_FORM[EVENT == '10K']),
      PR_FORM_10K = min(PR_FORM[EVENT == '10K']),
    ) %>%
    mutate_if(is.numeric, list(~na_if(., Inf))) # Replace Inf values created by runners not competing in an event
  
  # Return data frame
  return(df)
}

# Functions to get data for URL
# Meet results
getResultsData = function(url){
  # URL to scrape
  meet_url = url
  # Get webpage URLs for runners
  meet_wp = getResultsURLs(meet_url)
  # Query the runners results
  runners = resultsQuery(meet_wp)
  # Reformat the data
  runners = reformatRunners(runners)
  
  # Return final runners data frame
  return(runners)
}

# Performance lists
getPerfListData = function(url){
  # URL to scrape
  meet_url <- url
  # Get webpage URLs for runners
  meet_wp <- getPerfListURLs(meet_url)
  # Subset to distance events
  wp <- meet_wp[451:1050]
  # Remove duplicates
  wp <- unique(wp)
  # Query the runners results
  runners <- resultsQuery(wp)
  # Reformat the data
  runners <- reformatRunners(runners)
  
  # Return final runners data frame
  return(runners)
}

getSeconds <- function(time) {
  splitTime <- strsplit(time, ":")
  min <- as.numeric(splitTime[[1]][1])
  sec <- as.numeric(splitTime[[1]][2])
  min2sec <- min*60
  totalTime <- min2sec + sec
  return(totalTime)
}

createName <- function(name) {
  splitName <- strsplit(name, ", ")
  splitName <- unlist(splitName)
  newName <- paste0(splitName[2], " ", splitName[1])
  newName <- toupper(newName)
  newName <- str_trim(newName)
  return(newName)
}
