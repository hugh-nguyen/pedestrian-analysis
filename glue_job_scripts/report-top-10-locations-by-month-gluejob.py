# Top 10 locations by month

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
OUTPUT_TABLE_NAME = 'report_top_10_locations_by_month'

schema = StructType([
    StructField("year_month",StringType(),True),
    StructField("rank",IntegerType(),True),
    StructField("sensor_id",IntegerType(),True),
    StructField("location_name",StringType(),True),
    StructField("monthly_count",IntegerType(),True),
])

s3_path = f"s3://{BUCKET_NAME}/report/{OUTPUT_TABLE_NAME}/"
util.create_glue_catalog_table(DATABASE_NAME, OUTPUT_TABLE_NAME, schema, s3_path)

####  Load sensor_counts_by_day
sensor_count_by_day_df = glueContext.create_dynamic_frame.from_catalog(
    database="pedestrian_analysis_report",
    table_name="report_top_10_locations_by_day"
).toDF()

sensor_count_by_day_df.show(10, truncate=False)

####  Load sensor_reference_data
sensor_reference_df = glueContext.create_dynamic_frame.from_catalog(
    database="pedestrian_analysis_raw",
    table_name="sensor_reference_data"
).toDF()

sensor_reference_df.show(10)

####  'date' is currently a full date string
####  This extracts the year and month
sensor_count_by_day_df = sensor_count_by_day_df \
    .withColumn("year_month", col("date").substr(1, 7))

sensor_count_by_day_df.show(10, truncate=False)

#### Group by 'year_month' and 'sensor_id' and sum the 'daily_count' per group
grouped_sensor_count_df = sensor_count_by_day_df \
    .groupBy("year_month", "sensor_id") \
    .agg(sum("daily_count").alias("monthly_count"))

grouped_sensor_count_df.show(10, truncate=False)

#### Add a new column 'rank' that ranks the rows within each partition based on their monthly_count
window_spec = Window.partitionBy("year_month") \
    .orderBy(desc("monthly_count"))
ranked_sensor_count_df = grouped_sensor_count_df \
    .withColumn("rank", row_number().over(window_spec))

ranked_sensor_count_df.show(10, truncate=False)

#### Filter the rows where rank <= 10 to get the top 10 sensor_ids for each month
top_10_sensors_by_month_df = ranked_sensor_count_df.filter(col("rank") <= 10) \
    .orderBy(col("year_month").desc(), col("rank"))

top_10_sensors_by_month_df.show(10)

#### Left join the reference data in to obtain the correct sensor_description
top_10_sensors_by_month_df = top_10_sensors_by_month_df.join(
    sensor_reference_df,
    col("sensor_id") == col("location_id"),
    "left"
)

#### Select and format final report
top_10_sensors_by_month_df = top_10_sensors_by_month_df.select(
    col('year_month').cast('string'),
    col('rank'),
    col('sensor_id'),
    col('sensor_description').alias('location_name'),
    col('monthly_count').cast('int').alias('monthly_count')
)

top_10_sensors_by_month_df.show(100, truncate=False)

#### Upload to S3
util.upload_to_s3(glueContext, top_10_sensors_by_month_df, s3_path)