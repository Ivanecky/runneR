import pandas as pd
import psycopg2
from psycopg2 import sql
import requests
import yaml 
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

# Function to get event results from URL
def get_event_results(url):
    # Get HTML content from the URL
    response = requests.get(url)
    html = response.content

    # Parse HTML using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extract meet details
    meet_name = soup.select_one('h3 a').text.strip()

    # Get location info
    loc_info = soup.find_all(class_ = "panel-heading-normal-text inline-block")
    meet_date = loc_info[0].text
    meet_loc = loc_info[1].text
    meet_track_type = loc_info[2].text
    # Check if at altitude 
    if len(loc_info) > 3:
        meet_altitude = loc_info[3].text
    else: 
        meet_altitude = None 

    # Extract race names
    race_names = [h3.text.strip() for h3 in soup.find_all('h3')]
    race_names = race_names[1:]  # Drop the first race name (same as meet name)
    
    # Find all tables and convert them to DataFrames
    tables = pd.read_html(response.text)
    
    # Initialize an empty DataFrame for race results
    race_results = pd.DataFrame()

    for i, table in enumerate(tables):
        # Select specific columns (assuming PL, NAME, YEAR, TEAM, TIME exist in the table)
        temp_results = table[['PL', 'NAME', 'YEAR', 'TEAM', 'TIME']]
        
        # Add race name, check if race_names has enough elements to avoid index errors
        if i < len(race_names):
            temp_results['RACE_NAME'] = race_names[i]
        else:
            temp_results['RACE_NAME'] = 'Unknown Race'

        # Append to the main DataFrame
        race_results = pd.concat([race_results, temp_results], ignore_index=True)

    # Add meet details to the DataFrame
    race_results['MEET_DATE'] = meet_date
    race_results['MEET_LOCATION'] = meet_loc
    race_results['MEET_NAME'] = meet_name
    race_results['MEET_TRACK_TYPE'] = meet_track_type
    race_results['MEET_ALTITUDE'] = meet_altitude

    # Drop duplicates if necessary
    race_results = race_results.drop_duplicates()

    # Filter out any "UNKNOWN RACE" as these are duplicates of the prelim data
    race_results = race_results[race_results['RACE_NAME'] != 'Unknown Race']

    return race_results

# Read connection data from YAML
with open("/Users/samivanecky/git/runneR/secrets/aws_rds.yaml", "r") as yaml_file:
    pg_config = yaml.safe_load(yaml_file)

# Connect to the database
conn = psycopg2.connect(
    host=pg_config['host'],
    user=pg_config['user'],
    password=pg_config['pwd'],
    dbname=pg_config['dbname'],
    port=pg_config['port']
)

# Load data from event links table
evnt_links_query = "SELECT * FROM track_scraped_links"
evnt_links = pd.read_sql_query(evnt_links_query, conn)

# Get already scraped links
cmp_links_query = "SELECT * FROM track_scraped_links_comp"
cmp_links = pd.read_sql_query(cmp_links_query, conn)

# Remove scraped links from event links
evnt_links = evnt_links[~evnt_links['link'].isin(cmp_links['link'])]

# Get only 10,000 most recent links
evnt_links = evnt_links.head(10000)

# Convert links to a list
lnks = evnt_links['link'].unique().tolist()

# Function for fetching track results in parallel
def get_track_res_in_par(url):
    try:
        # Get event results
        tmp_res = get_event_results(url)
    except Exception as e:
        print(f"Error fetching data for: {url}")
        print(f"Error message: {e}")
        return None
    return tmp_res

all_res = []

# Loop through links and fetch results
for i, link in enumerate(lnks):
    print(f"Getting data for {link}, which is link {i+1} of {len(lnks)}")
    tmp_df = get_track_res_in_par(link)
    if tmp_df is not None:
        all_res.append(tmp_df)

# Concatenate all results into one DataFrame
track_res = pd.concat(all_res, ignore_index=True)

# Write data to the database table
with conn.cursor() as cursor:
    # Convert DataFrame to list of tuples
    track_res_values = [tuple(x) for x in track_res.to_numpy()]
    
    # Create INSERT query
    insert_query = sql.SQL("INSERT INTO ind_track_results_raw ({}) VALUES {}").format(
        sql.SQL(', ').join(map(sql.Identifier, track_res.columns)),
        sql.SQL(', ').join([sql.Placeholder()] * len(track_res.columns))
    )
    
    # Execute INSERT query
    cursor.executemany(insert_query, track_res_values)
    conn.commit()

# Write scraped links to the database table
with conn.cursor() as cursor:
    for _, row in evnt_links.iterrows():
        insert_query = sql.SQL("INSERT INTO track_scraped_links_comp (link) VALUES (%s)")
        cursor.execute(insert_query, (row['link'],))
    conn.commit()

# Close the database connection
conn.close()
