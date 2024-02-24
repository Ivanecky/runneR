from datetime import datetime
import pandas as pd

# Function for getting first date
def get_start_dt(meet_date) -> str:
    # Check if the date is multi or single month
    month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

    # Count the occurrences of each month name in the input string
    occurrences = sum(1 for month in month_names if month in meet_date)

    # Strip any potential whitespace
    meet_date = meet_date.strip()
    
    if occurrences < 2:
        # Split the meet date
        dt_pts = meet_date.split(' ')
        
        # Get the month index
        month_number = datetime.strptime(dt_pts[0], "%B").month
                
        # Extract day component
        day_pts = dt_pts[1].split('-')
                
        # Get first component as day
        day = day_pts[0].replace(",", "")
    
        # Get year
        yr = dt_pts[2]
    
        # Put all together
        start_dt = str(yr) + '-' + str(month_number) + '-' + str(day)
    
        # Return data
        return(start_dt)
    
    # Handle multi month dates
    else:
        dt_pts = meet_date.split('-')
    
        # Get second date for year component
        second_dt = datetime.strptime(dt_pts[1], "%B %d, %Y")
    
        # Combine year for first component
        first_dt = str(dt_pts[0]) + ', ' + str(second_dt.year)
    
        # Convert to datetime
        first_dt = datetime.strptime(first_dt, "%B %d, %Y")
    
        # Convert to proper string format
        first_dt = first_dt.strftime("%Y-%m-%d")
    
        # Return date
        return(first_dt)

# Function for getting second date   
def get_end_dt(meet_date) -> str:
    # Check if the date is multi or single month
    month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

    # Count the occurrences of each month name in the input string
    occurrences = sum(1 for month in month_names if month in meet_date)

     # Strip any potential whitespace
    meet_date = meet_date.strip()
    
    if occurrences < 2:
        return("NULL")
    
    # Handle multi month dates
    else:
        dt_pts = meet_date.split('-')
    
        # Get second date for year component
        second_dt = datetime.strptime(dt_pts[1], "%B %d, %Y")
    
        # Convert to proper string format
        second_dt = second_dt.strftime("%Y-%m-%d")
    
        # Return date
        return(second_dt)
    
# convert track to numeric length
def convert_track_length(track_length) -> int:
    if track_length != "NULL":
        # drop the potential m
        track_length = track_length.replace('m', '')

        # convert from string to int
        track_length = int(track_length)

        return(track_length)
    # handle null values
    else:
        return(400)