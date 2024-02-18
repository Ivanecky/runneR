# Change working directory
os.chdir('/Users/samivanecky/git/runneR/py-rnr/track-scraper/')

# load libraries
import pandas as pd
import yaml
import psycopg2
import rnr_fxns as rnr
import yaml
import warnings
from sqlalchemy import create_engine
import iceberg as ib 
import pyarrow as pa
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

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


    # create Spark session
    spark = SparkSession.builder \
        .appName("Write to RDS PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    # Write using Spark
    # Convert pandas -> spark
    all_res_sp = spark.createDataFrame(all_res)

    # Drop unnamed col
    all_res_sp = all_res_sp.drop("Unnamed: 0")

    # JDBC URL
    url = f"jdbc:postgresql://{pg_config['host']}:5432/{pg_config['dbname']}"

    # Set properties
    properties = {
        "user": pg_config['user'],
        "password": pg_config['pwd'],
        "driver": "org.postgresql.Driver"
    }

    # Before uploading, replace any NaN or NA with -1
    all_res_sp = all_res_sp.withColumn("pl", f.when(f.isnull(f.col("pl")) | f.isnan(f.col("pl")), -1).otherwise(f.col("pl")))

    # Write DataFrame to RDS PostgreSQL table
    all_res_sp.write.jdbc(url=url, table="tf_ind_res_fct", mode="append", properties=properties)