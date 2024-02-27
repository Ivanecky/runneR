# Change working directory
os.chdir('/Users/samivanecky/git/runneR/py-rnr/track-scraper/')

# load libraries
import pandas as pd
import yaml
import psycopg2
import rnr_fxns as rnr
import yaml
import warnings
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

    # Load meet links
    # For a historical reload, modify this to use the all_meet_lnks function
    meet_lnks = rnr.get_current_meet_lnks()

    # Remove any XC links
    meet_lnks = [l for l in meet_lnks if '/xc/' not in l]

    # Go to each meet link and get meet info
    for l in meet_lnks:
        # Get data for link
        try:
            print(f"Getting data for: {l}")
            tmp_df = rnr.get_meet_info(l)
            # Try and append to dataframe
            try:
                all_meet_info = pd.concat([all_meet_info, tmp_df], axis=0)
            except Exception as e:
                print("Dataframe doesn't exist yet. Creating base table...")
                # create new dataframe
                all_meet_info = tmp_df 
        except Exception as e:
            print(e)
    
    # Query data already in facilities table
    existing_fac = pd.read_sql("SELECT * FROM facilities", conn)

    # Join tables and drop any duplicates
    all_facs = pd.concat([existing_fac, all_meet_info])

    # Remove any dups
    all_facs = all_facs.drop_duplicates()

    # Replace None with 0 for elevation field
    all_facs['elevation'] = all_facs['elevation'].fillna(0)

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Write to RDS PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    # JDBC URL
    url = f"jdbc:postgresql://{pg_config['host']}:5432/{pg_config['dbname']}"

    # Set properties
    properties = {
        "user": pg_config['user'],
        "password": pg_config['pwd'],
        "driver": "org.postgresql.Driver"
    }

    # Convert pandas -> spark
    all_facs_sp = spark.createDataFrame(all_facs)

    # Write DataFrame to RDS PostgreSQL table
    all_facs_sp.write.jdbc(url=url, table="facilities", mode="overwrite", properties=properties)