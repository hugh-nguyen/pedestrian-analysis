import requests, sys, io, zipfile, pandas as pd, util
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CITY_OF_MELBOURNE_API_KEY', 'BUCKET_NAME'])

glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


URL = 'https://melbournetestbed.opendatasoft.com/api/v2/catalog/datasets/pedestrian-counting-system-monthly-counts-per-hour/attachments'
CITY_OF_MELBOURNE_API_KEY = args['CITY_OF_MELBOURNE_API_KEY']
ATTACHMENT_NAME = 'pedestrian_counting_system_monthly_counts_per_hour_may_2009_to_14_dec_2022_csv_zip'
LOCAL_CSV_DOWNLOAD_NAME = 'Pedestrian_Counting_System_Monthly_counts_per_hour_may_2009_to_14_dec_2022.csv'

BUCKET_NAME = args['BUCKET_NAME']
DATABASE_NAME = 'pedestrian_analysis_raw'
TABLE_NAME = 'sensor_counts'

s3_path = f"s3://{BUCKET_NAME}/raw/{TABLE_NAME}/"

# Define the schema for the table
schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("date_time",StringType(),True),
    StructField("sensor_id",IntegerType(),True),
    StructField("sensor_name",StringType(),True),
    StructField("hourly_count",IntegerType(), True),
])

util.create_glue_catalog_table(DATABASE_NAME, TABLE_NAME, schema, s3_path)

response = util.city_of_melbourne_api_request(URL, CITY_OF_MELBOURNE_API_KEY)

# find the attachment with the specified ID
attachment = next(
    attachment for attachment in response.json()['attachments']
    if attachment['metas']['id'] == ATTACHMENT_NAME
)

# make a GET request to the attachment URL and 
# load the content of the attachment into memory as a ZipFile object
attachment_response = requests.get(attachment['href'])
attachment_zipfile = zipfile.ZipFile(io.BytesIO(attachment_response.content))

# Load the CSV data into a Pandas DataFrame and preprocess it
df = pd.read_csv(attachment_zipfile.open(LOCAL_CSV_DOWNLOAD_NAME))
df = df.loc[:, ['ID', 'Date_Time', 'Sensor_ID', 'Sensor_Name', 'Hourly_Counts']]
df['Date_Time'] = pd.to_datetime(df['Date_Time'], format='%B %d, %Y %I:%M:%S %p') \
    .dt.strftime('%Y-%m-%dT%H:%M:%S')

# Convert the Pandas DataFrame to a PySpark DataFrame
spark_df = spark.createDataFrame(df, schema=schema)

util.upload_to_s3(glueContext, spark_df, s3_path)

