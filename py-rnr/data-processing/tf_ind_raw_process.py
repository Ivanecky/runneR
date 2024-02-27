# Change working directory
os.chdir('/Users/samivanecky/git/runneR/py-rnr/data-processing/')

# load libraries
import pandas as pd
import yaml
import yaml
import warnings
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import processing_fxns as pf
from pyspark.sql.types import StringType, IntegerType, FloatType

# Ignore FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)
pd.options.mode.copy_on_write = True


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
	team as team_name,
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
		when time = 'Z' then 0
		else -- If no ':', assume the value is already in seconds
		time::FLOAT
	end as time_in_seconds,
	meet_date, 
	meet_location,
	meet_name,
	coalesce(elevation, '0') as elevation,
	coalesce(track_length, '400m') as track_length,
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
convert_track_len_udf = f.udf(pf.convert_track_length, IntegerType())
get_track_type_conv_udf = f.udf(pf.get_track_conv_type, StringType())
get_altitude_conv_udf = f.udf(pf.get_conv, FloatType())

# Call UDFs on df

tf_ind = tf_ind.withColumn("meet_start_dt", get_start_dt_udf(f.col("meet_date"))) \
	.withColumn("meet_end_dt", get_end_dt_udf(f.col("meet_date"))) \
	.withColumn("track_length", convert_track_len_udf(f.col("track_length")))

# Query team info
team_divs = spark.read \
	.format("jdbc") \
	.option("url", url) \
	.option("user", pg_config['user']) \
	.option("password", pg_config['pwd']) \
	.option("query", 'SELECT * FROM team_lookup_info') \
	.option("driver", "org.postgresql.Driver") \
	.load()

# Size query
sz_qry = '''
	select 
		case 
			when gender like 'men' then 'M'
			else 'F'
		end as gender, 
		event as evnt, 
		type, 
		conversion 
	from 
		ncaa_size_conv_dim 
'''

# Query conversion table
size_conv = spark.read \
	.format("jdbc") \
	.option("url", url) \
	.option("user", pg_config['user']) \
	.option("password", pg_config['pwd']) \
	.option("query", sz_qry) \
	.option("driver", "org.postgresql.Driver") \
	.load()

# Join team divs to overall table
tf_ind = tf_ind.join(
	team_divs, 
	f.upper(tf_ind['team_name']) == team_divs['TEAM'],
	how='left'
)

# Drop duplicate field
tf_ind = tf_ind.drop('TEAM')

# Create a field for track size join
tf_ind = tf_ind.withColumn("track_conv_type", get_track_type_conv_udf(f.col("track_length"), f.col("banked_or_flat")))

# Join track size conversion
tf_ind = tf_ind.join(
	size_conv,
	(f.lower(tf_ind['event']) == size_conv['evnt']) & (tf_ind['gender'] == size_conv['gender']) & (tf_ind['track_conv_type'] == size_conv['type']),
	how='left'
).drop("evnt")

# Convert NULL values to 1
tf_ind = tf_ind.withColumn("conversion", f.coalesce(f.col('conversion'), f.lit(1)))

# Generate altitude converted mark & fully converted mark
tf_ind_conv = tf_ind.withColumn("alt_conv_mark", get_altitude_conv_udf(f.col("elevation"), f.col("event"), f.col("time_in_seconds"))) \
					.withColumn("converted_mark", f.col("alt_conv_mark") * f.col("conversion"))