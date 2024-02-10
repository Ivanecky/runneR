from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests 
import pandas as pd 
import uuid
import psycopg2
import yaml
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

pd.options.mode.copy_on_write = True

# Code to load the URLs to be batched
# Connect to postgres
# Read connection data from YAML
with open("/opt/airflow/secrets/aws_rds.yaml", "r") as yaml_file:
    pg_config = yaml.safe_load(yaml_file)

# Connect to the database
conn = psycopg2.connect(
    host=pg_config['host'],
    user=pg_config['user'],
    password=pg_config['pwd'],
    dbname=pg_config['dbname'],
    port=pg_config['port']
)

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
    # Check for track size (outdoor doesn't list sizing)
    if len(loc_info) > 2:
        meet_track_type = loc_info[2].text
    else:
        meet_track_type = None
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

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('batch_url_processing',
          default_args=default_args,
          description='Process TFRRS URLs in batches',
          max_active_runs=1,
          max_active_tasks=6,
          schedule_interval=None,  # None means this DAG will only be triggered manually
          catchup=False)

def process_batch(batch_of_urls, conn, **kwargs):
    """
    Function to process a batch of URLs.
    Replace the content with your actual scraping logic.
    """
    # Your scraping logic here
    print(f"Processing {len(batch_of_urls)} URLs")

    # Iterate over URLs and get the data
    for url in batch_of_urls:
        # print status
        print(f"Getting data for {url}")

        try:
            # get the event data
            tmp_res = get_event_results(url)
            # combine the data
            try:
                # concat data with existing data
                all_res = pd.concat([all_res, tmp_res], axis=0)
            except Exception as e:
                print(f"{e}")
                all_res = tmp_res
        except Exception as e:
            print(f"{e}")
    
    # # Once data is filled, write data to database
    # fp = '/opt/airflow/data/' + str(uuid.uuid4()) + '.csv'
    # all_res.to_csv(fp)
            
    print("Loading data to postgres...")

    # connection_str = 'postgresql://' + str(pg_config['user']) + ':' + str(pg_config['pwd']) + '@' + str(pg_config['host']) + ':5432/' + str(pg_config['dbname'])

    # Create a SQLAlchemy engine
    engine = create_engine(conn)

    # Write the DataFrame to the PostgreSQL database
    all_res.to_sql('tf_ind_lines_fct', engine, if_exists='append', index=False)

# Load data from event links table
evnt_links_query = "SELECT * FROM track_scraped_links"
evnt_links = pd.read_sql(evnt_links_query, conn)

# Get only the links
lnks = list(evnt_links['link'])

# Assuming you've split your URLs into batches
batches = [lnks[i:i + 1000] for i in range(0, len(lnks), 1000)]

for i, batch in enumerate(batches):
    task = PythonOperator(
        task_id=f'process_batch_{i}',
        python_callable=process_batch,
        op_kwargs={'batch_of_urls': batch},
        dag=dag
    )
