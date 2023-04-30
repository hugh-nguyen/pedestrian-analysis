# Top 10 locations by day

####  Import packages and start the session

import sys, io, zipfile, pandas as pd, util
from datetime import datetime

from pyspark.sql.functions import sum, col, rank, desc, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CITY_OF_MELBOURNE_API_KEY', 'BUCKET_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

####  Create output glue table if it doesn't already exist
##### The results of this notebook will be loaded into this table

BUCKET_NAME = args['BUCKET_NAME']
DATABASE_NAME = 'pedestrian_analysis_report'
OUTPUT_TABLE_NAME = 'report_top_10_locations_by_day'

schema = StructType([
    StructField("date",StringType(),True),
    StructField("rank",IntegerType(),True),
    StructField("sensor_id",IntegerType(),True),
    StructField("location_name",StringType(),True),
    StructField("daily_count",IntegerType(),True),
])

s3_path = f"s3://{BUCKET_NAME}/report/{OUTPUT_TABLE_NAME}/"
util.create_glue_catalog_table(DATABASE_NAME, OUTPUT_TABLE_NAME, schema, s3_path)

####  Load sensor_counts
sensor_count_df = glueContext.create_dynamic_frame.from_catalog(
    database="pedestrian_analysis_raw",
    table_name="sensor_counts"
).toDF()

sensor_count_df.show(10, truncate=False)

####  Load sensor_reference_data
sensor_reference_df = glueContext.create_dynamic_frame.from_catalog(
    database="pedestrian_analysis_raw",
    table_name="sensor_reference_data"
).toDF()

sensor_reference_df.show(10)


####  'date_time' is currently a full iso formatted string
####  This converts date_time to a timestamp and then to a date

sensor_count_df = sensor_count_df \
    .withColumn("date_time", col("date_time").cast("timestamp")) \
    .withColumn("date", col("date_time").cast("date"))

sensor_count_df.show(10, truncate=False)

#### Group by 'date' and 'sensor_id' and sum the 'hourly_counts' per group
grouped_sensor_count_df = sensor_count_df \
    .groupBy("date", "sensor_id") \
    .agg(sum("hourly_count").alias("daily_count"))

grouped_sensor_count_df.show(10, truncate=False)

#### Add a new column 'rank' that ranks the rows within each partition based on their daily_count
window_spec = Window.partitionBy("date") \
    .orderBy(desc("daily_count"))
ranked_sensor_count_df = grouped_sensor_count_df \
    .withColumn("rank", row_number().over(window_spec))

ranked_sensor_count_df.show(10, truncate=False)

#### Filter the rows where rank <= 10 to get the top 10 sensor_ids for each day
top_10_sensors_by_day_df = ranked_sensor_count_df.filter(col("rank") <= 10) \
    .orderBy(col("date").desc(), col("rank"))

top_10_sensors_by_day_df.show(10)

#### Left join the reference data in to obtain the correct sensor_description
top_10_sensors_by_day_df = top_10_sensors_by_day_df.join(
    sensor_reference_df,
    col("sensor_id") == col("location_id"),
    "left"
)

#### Select and format final report
top_10_sensors_by_day_df = top_10_sensors_by_day_df.select(
    col('date').cast('string'),
    col('rank').cast('int'),
    col('sensor_id').cast('int'),
    col('sensor_description').alias('location_name'),
    col('daily_count').cast('int').alias('daily_count')
)

top_10_sensors_by_day_df.show(100, truncate=False)

#### Upload to S3
util.upload_to_s3(glueContext, top_10_sensors_by_day_df, s3_path)
