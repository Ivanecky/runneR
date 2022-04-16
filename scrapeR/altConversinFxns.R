# Build linear models for predicting size
library(tidymodels)
library(dplyr)

# Functions to get conversion factors based on elevations
getConv800 <- function(alt) {
  convMark = 1.004556e+00 + alt * (-2.137286e-06)
  return(convMark)
}

getConvMile <- function(alt) {
  convMark = 1.012194e+00 + alt * (-6.834575e-06)
  return(convMark)
}

getConv3k <- function(alt) {
  convMark = 1.015564e+00 + alt * (-8.205417e-06)
  return(convMark)
}

getConv5k <- function(alt) {
  convMark = 1.016327e+00 + alt * (-8.773153e-06)
  return(convMark)
}

# Create final altitude conversion
getConv <- function(alt, event, mark) {
  # Check if no elevation, return time as is
  if(alt == 0 | !(event %in% c("800m", "Mile", "1500m", "3000m", "5000m"))) {
    return(mark)
  } else {
    if(event == "800m") {
      # Get conversion factor
      convFac = getConv800(alt)
      # Convert mark
      convMrk = convFac * mark
      # Return converted mark
      return(convMrk)
    } else if(event == "Mile") {
      # Get conversion factor
      convFac = getConvMile(alt)
      # Convert mark
      convMrk = convFac * mark
      # Return converted mark
      return(convMrk)
    } else if(event == "3000m") {
      # Get conversion factor
      convFac = getConv3k(alt)
      # Convert mark
      convMrk = convFac * mark
      # Return converted mark
      return(convMrk)
    } else { # 5000m
      # Get conversion factor
      convFac = getConv5k(alt)
      # Convert mark
      convMrk = convFac * mark
      # Return converted mark
      return(convMrk)
    }
  }
}

# Function to return times in human readble format
reformatTimes <- function(mark) {
  sec <- str_pad((mark %% 60), width = 2, side = "left", pad = "0")
  min <- floor(mark / 60)
  time <- paste0(min, ":", sec)
  return(time)
}

# Work with field event data
handleFieldEvents <- function(mark) {
  # Check for meters
  if (grepl("m", mark)) {
    newMark <- as.numeric(gsub("m", "", mark))
    return(newMark)
  } else if (grepl("'", mark)) { # Check for feet
    newMark <- as.numeric(gsub("'", "", mark)) * 0.3048
    return(newMark)
  } else { # Handle all other situations
    return(-999)
  }
}

# Converts marks to numeric values
convertMarks <- function(is_field, mark) {
  MARK_TIME = case_when(
    is_field == TRUE ~ handleFieldEvents(mark),
    T ~ as.numeric(mark)
  )
  return(mark)
}