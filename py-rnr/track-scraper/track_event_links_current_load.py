# load libraries
import pandas as pd
import yaml
import psycopg2
import rnr_fxns as rnr
import yaml
import warnings
from sqlalchemy import create_engine

# Ignore FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)
pd.options.mode.copy_on_write = True

def main():
    # Connect to postgres
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
            
    # get all links on tfrrs
    meet_lnks = rnr.get_current_meet_lnks()

    # Get all event links from the meets
    evnt_lnks = rnr.get_tf_evnt_lnks(meet_lnks)

    # Get list of already queried links from pg
    # Load data from event links table
    scraped_evnt_links_query = "SELECT * FROM track_scraped_links_comp"
    scraped_evnt_lnks = pd.read_sql(scraped_evnt_links_query, conn)
    scraped_evnt_lnks = list(scraped_evnt_lnks['link'])

    # Filter out already scraped events
    evnt_lnks = [l for l in evnt_lnks if l not in scraped_evnt_lnks]

    # Iterate over events and get results
        # Iterate over URLs and get the data
    for url in evnt_lnks:
        # print status
        print(f"Getting data for {url}")

        try:
            # get the event data
            tmp_res = rnr.get_tf_event_results(url)
            # combine the data
            try:
                # concat data with existing data
                all_res = pd.concat([all_res, tmp_res], axis=0)
            except Exception as e:
                print(f"{e}")
                all_res = tmp_res
        except Exception as e:
            print(f"{e}")

    
    connection_str = 'postgresql://' + str(pg_config['user']) + ':' + str(pg_config['pwd']) + '@' + str(pg_config['host']) + ':5432/' + str(pg_config['dbname'])

    # Create a SQLAlchemy engine
    engine = create_engine(connection_str)

    # Create raw connection
    sql_conn = engine.connect()
            
    # Convert column names to lowercase to match table
    all_res.columns = all_res.columns.str.lower()

    # Write the DataFrame to the PostgreSQL database
    all_res.to_sql('tf_ind_res_fct ', sql_conn.connection, if_exists='append', index=False)