import sys, io, zipfile, pandas as pd, util
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CITY_OF_MELBOURNE_API_KEY', 'BUCKET_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

URL = 'https://melbournetestbed.opendatasoft.com/api/v2/catalog/datasets/pedestrian-counting-system-sensor-locations/exports/json'
CITY_OF_MELBOURNE_API_KEY = args['CITY_OF_MELBOURNE_API_KEY']

BUCKET_NAME = args['BUCKET_NAME']
DATABASE_NAME = 'pedestrian_analysis_raw'
TABLE_NAME = 'sensor_reference_data'

s3_path = f"s3://{BUCKET_NAME}/raw/{TABLE_NAME}/"

# Define the schema for the table
schema = StructType([
    StructField("location_id",IntegerType(),True),
    StructField("sensor_description",StringType(),True),
    StructField("sensor_name",StringType(),True),
    StructField("installation_date",StringType(),True),
    StructField("note",StringType(), True),
    StructField("location_type",StringType(),True),
    StructField("status",StringType(),True),
    StructField("direction_1",StringType(),True),
    StructField("direction_2",StringType(),True),
    StructField("latitude",DoubleType(), True),
    StructField("longitude",DoubleType(), True),
    StructField("location",StructType([
        StructField("lon",DoubleType(),True),
        StructField("lat",DoubleType(),True),
    ]),True),
])

util.create_glue_catalog_table(DATABASE_NAME, TABLE_NAME, schema, s3_path)

response = util.city_of_melbourne_api_request(URL, CITY_OF_MELBOURNE_API_KEY)

rdd = sc.parallelize(response.json())
spark_df = spark.createDataFrame(rdd)

util.upload_to_s3(glueContext, spark_df, s3_path)
