import sys, io, util
from datetime import datetime, timedelta

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

from pyspark.context import SparkContext
from pyspark.sql.functions import sum, col, rank, asc, lit, when
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

####  Create output glue table if it doesn't already exist
##### The results of this notebook will be loaded into this table

BUCKET_NAME = 'pedestrian-analysis-working-bucket'
DATABASE_NAME = 'pedestrian_analysis_report'
OUTPUT_TABLE_NAME = 'report_most_growth_last_year'

schema = StructType([
    StructField("location_name", StringType(), True),
    StructField("count_previous_year", IntegerType(), True),
    StructField("count_last_year", IntegerType(), True),
    StructField("growth", IntegerType(), True),
    StructField("growth_percent", DoubleType(), True),
])

s3_path = f"s3://{BUCKET_NAME}/report/{OUTPUT_TABLE_NAME}/"
util.create_glue_catalog_table(DATABASE_NAME, OUTPUT_TABLE_NAME, schema, s3_path)

####  Load sensor_counts
sensor_counts_df = glueContext.create_dynamic_frame.from_catalog(
    database="pedestrian_analysis_raw",
    table_name="sensor_counts"
).toDF()

sensor_counts_df.show(10, truncate=False)

####  Load sensor_reference_data
sensor_reference_df = glueContext.create_dynamic_frame.from_catalog(
    database="pedestrian_analysis_raw",
    table_name="sensor_reference_data"
).toDF()

sensor_reference_df.show(10)

#### Calculate sensor counts 2019
##### Because the cutsoff at 2022-11-01, for the purpose of this analysis we are setting the end of each year to November 11
sensor_counts_2019_df = sensor_counts_df \
    .filter(col('date_time') >= '2018-11-01') \
    .filter(col('date_time') < '2019-11-01') \
    .groupBy('sensor_id') \
    .agg(sum('hourly_count').alias('count_2019'))

sensor_counts_2019_df.show(10)

#### Calculate sensor counts 2022
sensor_counts_2022_df = sensor_counts_df \
    .filter(col('date_time') >= '2021-11-01') \
    .filter(col('date_time') < '2022-11-01') \
    .groupBy('sensor_id') \
    .agg(sum('hourly_count').alias('count_2022'))

sensor_counts_2022_df.show(10)

#### Calculate the decline and decline percentages for each sensor
sensor_decline_df = sensor_counts_2019_df \
    .join(sensor_counts_2022_df, on='sensor_id', how='inner') \
    .withColumn('decline', (col('count_2019') - col('count_2022'))) \
    .withColumn(
        'decline_percent',
        ((col('count_2019') - col('count_2022')) / col('count_2019')) * 100
    )

sensor_decline_df.show(10)

#### Join reference and select relevant columns
sensor_decline_df = sensor_decline_df.join(
    sensor_reference_df,
    col("sensor_id") == col("location_id"),
    "left"
)

sensor_decline_df = sensor_decline_df.select(
    col('sensor_id'),
    col('sensor_description').alias('location_name'),
    col('count_2019').cast('int'),
    col('count_2022').cast('int'),
    col('decline'),
    col('decline_percent')
).orderBy(
    desc('decline_percent')
)

sensor_decline_df.show(100, truncate=False)

#### Upload to S3
util.upload_to_s3(glueContext, sensor_decline_df, s3_path)