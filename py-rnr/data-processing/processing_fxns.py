from datetime import datetime
import re 

# Function for getting first date
def get_start_dt(meet_date) -> str:
    # Check if the date is multi or single month
    month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

    # Count the occurrences of each month name in the input string
    occurrences = sum(1 for month in month_names if month in meet_date)

    # Strip any potential whitespace
    meet_date = meet_date.strip()

    # Remove any duplicate whitespace
    meet_date = re.sub(r'\s+', ' ', meet_date)

    # Replace any hyphens followed by a space with just a hyphen
    # Some rows have weird formatting with an extraneous space that isn't technically a double space
    meet_date = re.sub('- ', '-', meet_date)

    try:
    
        # Handle multi month dates
        if occurrences > 1:
            # Split into separate months
            dt_pts = meet_date.split('-')

            # Grab the year
            meet_yr = meet_date[-4:]

            # Get the first component and append the year
            date_string = str(dt_pts[0]) + ', ' + str(meet_yr)

            # Convert to date and then string format
            date_string = datetime.strptime(date_string, "%B %d, %Y")

            # Format as string
            first_dt = date_string.strftime("%Y-%m-%d")
        
        # Handle single month dates
        else:

            # Check if date is multiple days or single day
            if '-' in meet_date: # Multiple days
                # Split off year
                meet_yr = meet_date[-4:]

                # Split the date on the comma
                days_pts = meet_date.split(',')

                # Split into month and days
                mnth_dy = days_pts[0].split(' ')
                
                # Get the first day
                day_one = mnth_dy[1].split('-')[0]

                # Put the components together in a single string
                date_string = str(mnth_dy[0]) + ' ' + str(day_one) + ', ' + str(meet_yr)
                date_string = datetime.strptime(date_string, "%B %d, %Y")

                # Format as string
                first_dt = date_string.strftime("%Y-%m-%d")

            else: # Single day
                # Convert the string to a date
                date_string = datetime.strptime(meet_date, "%B %d, %Y")

                # Format as string
                first_dt = date_string.strftime("%Y-%m-%d")
                
        # Return date
        return(first_dt)
    except Exception as e:
        return("ERROR")

# Function for getting second date   
def get_end_dt(meet_date) -> str:
    # Check if the date is multi or single month
    month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

    # Count the occurrences of each month name in the input string
    occurrences = sum(1 for month in month_names if month in meet_date)

    # Strip any potential whitespace
    meet_date = meet_date.strip()

    # Remove any duplicate whitespace
    meet_date = re.sub(r'\s+', ' ', meet_date)

    # Replace any hyphens followed by a space with just a hyphen
    # Some rows have weird formatting with an extraneous space that isn't technically a double space
    meet_date = re.sub('- ', '-', meet_date)
    
    try:
        # Handle multi month dates
        if occurrences > 1:
            # Split into separate months
            dt_pts = meet_date.split('-')

            # Convert to date and then string format
            date_string = datetime.strptime(dt_pts[1], "%B %d, %Y")

            # Format as string
            second_dt = date_string.strftime("%Y-%m-%d")
        
        # Handle single month dates
        else:
            # Check if date is multiple days or single day
            if '-' in meet_date: # Multiple days
                # Split off year
                meet_yr = meet_date[-4:]

                # Split the date on the comma
                days_pts = meet_date.split(',')

                # Split into month and days
                mnth_dy = days_pts[0].split(' ')
                
                # Get the second day
                day_two = mnth_dy[1].split('-')[1]

                # Put the components together in a single string
                date_string = str(mnth_dy[0]) + ' ' + str(day_two) + ', ' + str(meet_yr)
                date_string = datetime.strptime(date_string, "%B %d, %Y")

                # Format as string
                second_dt = date_string.strftime("%Y-%m-%d")

            else: # Single day
                # Single day meet means null second date
                second_dt = 'NULL'
                
        # Return date
        return(second_dt)

    except Exception as e:
        return("ERROR")
    
# convert track to numeric length
def convert_track_length(track_length) -> int:
    if track_length != "NULL" and track_length is not None and track_length != '':
        # drop the potential m
        track_length = track_length.replace('m', '')

        # convert from string to int
        track_length = int(track_length)

        return(track_length)
    # handle null values
    else:
        return(400)

# Function for altitude conversions
def get_conv(alt, event, mark) -> float:
    # Check if no elevation, return time as is
    if alt == 0 or alt == '' or event not in ["800m", "Mile", "3000m", "5000m"]:
        return mark
    else:
        if event == "800m":
            # Get conversion factor
            conv_fac = 1.004556e+00 + float(alt) * (-2.137286e-06)
        elif event == "Mile":
            # Get conversion factor
            conv_fac = 1.012194e+00 + float(alt) * (-6.834575e-06)
        elif event == "3000m":
            # Get conversion factor
            conv_fac = 1.015564e+00 + float(alt) * (-8.205417e-06)
        else:  # 5000m
            # Get conversion factor
            conv_fac = 1.016327e+00 + float(alt) * (-8.773153e-06)
        
        # Convert mark
        conv_mrk = float(conv_fac) * float(mark)

        # Return converted mark
        return round(conv_mrk, 2)

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