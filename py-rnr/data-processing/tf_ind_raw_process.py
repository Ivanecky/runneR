# Change working directory
os.chdir('/Users/samivanecky/git/runneR/py-rnr/data-processing/')

# load libraries
from bs4 import MarkupResemblesLocatorWarning
import pandas as pd
import yaml
import psycopg2
import yaml
import warnings
from sqlalchemy import create_engine
import iceberg as ib 
import pyarrow as pa
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import processing_fxns as pf
from pyspark.sql.types import StringType

# Ignore FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)
pd.options.mode.copy_on_write = True

def main():
    # Connect to postgres
    with open("/Users/samivanecky/git/runneR/secrets/aws_rds.yaml", "r") as yaml_file:
        pg_config = yaml.safe_load(yaml_file)

    # Setup spark session
    spark = SparkSession.builder \
        .appName("Write to RDS PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memoryOverhead", "4g") \
        .config("spark.driver.memoryOverhead", "4g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    # JDBC URL
    url = f"jdbc:postgresql://{pg_config['host']}:5432/{pg_config['dbname']}"

    # Define query for getting raw data
    qry = """
	with tmp as (
		select name,
			pl,
			team,
			case
				when lower(year) like '%fr%'
				or lower(year) like '%freshman%' then 'FR'
				when lower(year) like '%so%'
				or lower(year) like '%sophomore%' then 'SO'
				when lower(year) like '%jr%'
				or lower(year) like '%junior%' then 'JR'
				when lower(year) like '%sr%'
				or lower(year) like '%senior%' then 'SR'
				else 'OTHER'
			end as class,
			case
				when lower(race_name) like '%55%'
				and lower(race_name) not like '%hurdles%' then '55m'
				when lower(race_name) like '%60%%'
				and lower(race_name) not like '%hurdles%'
				and lower(race_name) not like '%600%' then '60m'
				when lower(race_name) like '%55%'
				and lower(race_name) like '%hurdles%' then '55mH'
				when lower(race_name) like '%60%'
				and lower(race_name) like '%hurdles%' then '60mH'
				when lower(race_name) like '%100%'
				and lower(race_name) not like '%hurdles%'
				and lower(race_name) not like '%1000%' then '100m'
				when lower(race_name) like '%100%'
				and lower(race_name) like '%hurdles%'
				and lower(race_name) not like '%1000%' then '100mH'
				when lower(race_name) like '%110%'
				and lower(race_name) like '%hurdles%' then '110mH'
				when lower(race_name) like '%200%' then '200m'
				when lower(race_name) like '%300%'
				and lower(race_name) not like '%3000%' then '300m'
				when lower(race_name) like '%400%'
				and lower(race_name) not like '%hurdles%' then '400m'
				when lower(race_name) like '%400%'
				and lower(race_name) like '%hurdles%' then '400mH'
				when lower(race_name) like '%500%'
				and lower(race_name) not like '%5000%' then '500m'
				when lower(race_name) like '%600%' then '600m'
				when lower(race_name) like '%800%'
				and lower(race_name) not like '%8000%' then '800m'
				when (
					lower(race_name) like '%1000%'
					or lower(race_name) like '%1,000%'
				)
				and lower(race_name) not like '%10000%' then '1000m'
				when lower(race_name) like '%1500%'
				or lower(race_name) like '%1,500%' then '1500m'
				when lower(race_name) like '%mile%' then 'Mile'
				when (
					lower(race_name) like '%3000%'
					or lower(race_name) like '%3,000%'
				)
				and lower(race_name) not like '%steeple%' then '3000m'
				when (
					lower(race_name) like '%3000%'
					or lower(race_name) like '%3,000%'
				)
				and lower(race_name) like '%steeple%' then '3000mS'
				when lower(race_name) like '%5000%'
				or lower(race_name) like '%5k%'
				or lower(race_name) like '%5,000%' then '5000m'
				when lower(race_name) like '%10000%'
				or lower(race_name) like '%10k%'
				or lower(race_name) like '%10,000%' then '10km'
				else 'OTHER'
			end as event,
			-- get gender from event name
			case 
				when lower(race_name) like '%men%' and lower(race_name) not like '%women%' then 'M'
				else 'F'
			end as gender,
			race_name, 
			-- check if race is a prelim
			case
				when lower(race_name) like '%prelim%' then 1
				else 0
			end as is_prelim,
			-- keep raw time for display purposes
			REPLACE(time, 'h', '0') as time,
			meet_date,
			meet_location,
			meet_name,
			f.elevation,
			f.track_length,
			f.banked_or_flat
		from tf_ind_res_fct t
			left join facilities f on t.meet_location = f.meet_facility
	)

	select name,
		pl,
		team,
		class,
		event,
		race_name, 
		is_prelim, 
		gender, 
		time,
		case
			-- Check if the string contains ':'
			when position(':' in time) > 0 then -- Extract minutes and convert to seconds
			(split_part(time, ':', 1)::INTEGER * 60) + -- Extract seconds and possibly milliseconds
			split_part(time, ':', 2)::FLOAT
			when time = 'DNF' then 0
			when time = 'FS' then 0
			when time = 'DNS' then 0
			when time = 'DQ' then 0
			when time = 'NT' then 0
			else -- If no ':', assume the value is already in seconds
			time::FLOAT
		end as time_in_seconds,
		meet_date, 
		meet_location,
		meet_name,
		elevation,
		track_length,
		banked_or_flat
	from tmp

    """

    # Read in data
    tf_ind = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("user", pg_config['user']) \
        .option("password", pg_config['pwd']) \
        .option("query", qry) \
        .option("driver", "org.postgresql.Driver") \
        .load() 
    
	# Register UDFs
    get_start_dt_udf = f.udf(pf.get_start_dt, StringType())
    get_end_dt_udf = f.udf(pf.get_end_dt, StringType())
    
	# Call UDFs on df

	tf_ind = tf_ind.withColumn("meet_start_dt", get_start_dt_udf(f.col("meet_date"))) \
    	.withColumn("meet_end_dt", get_end_dt_udf(f.col("meet_date")))

	tf_ind.show() 