from datetime import datetime

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
    if track_length != "NULL" and track_length is not None:
        # drop the potential m
        track_length = track_length.replace('m', '')

        # convert from string to int
        track_length = int(track_length)

        return(track_length)
    # handle null values
    else:
        return(400)
    
def get_conv_800(alt):
    conv_mark = 1.004556e+00 + alt * (-2.137286e-06)
    return conv_mark

def get_conv_mile(alt):
    conv_mark = 1.012194e+00 + alt * (-6.834575e-06)
    return conv_mark

def get_conv_3k(alt):
    conv_mark = 1.015564e+00 + alt * (-8.205417e-06)
    return conv_mark

def get_conv_5k(alt):
    conv_mark = 1.016327e+00 + alt * (-8.773153e-06)
    return conv_mark

def get_conv(alt, event, mark) -> float:
    # Check if no elevation, return time as is
    if alt == 0 or event not in ["800m", "Mile", "3000m", "5000m"]:
        return mark
    else:
        if event == "800m":
            # Get conversion factor
            conv_fac = get_conv_800(alt)
        elif event == "Mile":
            # Get conversion factor
            conv_fac = get_conv_mile(alt)
        elif event == "3000m":
            # Get conversion factor
            conv_fac = get_conv_3k(alt)
        else:  # 5000m
            # Get conversion factor
            conv_fac = get_conv_5k(alt)
        
        # Convert mark
        conv_mrk = conv_fac * mark
        # Return converted mark
        return conv_mrk

# function to determine track type for conversion
def get_track_conv_type(track_length, banked_or_flat) -> str:
    # Check to see if track is banked
    if banked_or_flat == 'banked':
        return('no conversion')
    elif banked_or_flat == 'flat' and track_length < 200:
        return('undersized')
    elif track_length == 200 and banked_or_flat == 'flat':
        return('flat')
    else:
        return('no conversion')