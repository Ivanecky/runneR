# Load the NCAA size conversions to postgres table
# Change working directory
os.chdir('/Users/samivanecky/git/runneR/')

# load libraries
import pandas as pd
import yaml
import psycopg2
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

    # Read in CSV file
    convs = pd.read_csv('ncaa_size_conversion.csv')

    # Load to postgres
    # create Spark session
    spark = SparkSession.builder \
        .appName("Write to RDS PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    # Write using Spark
    # Convert pandas -> spark
    convs_sp = spark.createDataFrame(convs)

    # JDBC URL
    url = f"jdbc:postgresql://{pg_config['host']}:5432/{pg_config['dbname']}"

    # Set properties
    properties = {
        "user": pg_config['user'],
        "password": pg_config['pwd'],
        "driver": "org.postgresql.Driver"
    }

    # Write DataFrame to RDS PostgreSQL table
    convs_sp.write.jdbc(url=url, table="ncaa_size_conv_dim", mode="append", properties=properties)