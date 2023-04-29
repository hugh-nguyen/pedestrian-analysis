import requests, boto3
from awsglue.dynamicframe import DynamicFrame


def create_glue_catalog_table(database_name, table_name, schema, s3_path):
    # Create a catalog client using the boto3 library
    client = boto3.client('glue')

    # Check if the table exists in the Glue Data Catalog
    try:
        response = client.get_table(DatabaseName=database_name, Name=table_name)
        print(f'Table {database_name}.{table_name} already exists in the Glue Data Catalog')
    except client.exceptions.EntityNotFoundException:
        print(f'Table {database_name}.{table_name} not found in the Glue Data Catalog. Creating table...')
        
        # Create the table in the Glue Data Catalog
        storage_descriptor = {
            'Columns': [{'Name': f.name, 'Type': f.dataType.simpleString()} for f in schema.fields],
            'Location': s3_path,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            },
            'Compressed': False
        }
        client.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': storage_descriptor,
                'PartitionKeys': [],
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {'classification': 'parquet'}
            }
        )
        print(f'Table {database_name}.{table_name} created in the Glue Data Catalog')


def upload_to_s3(glueContext, spark_df, s3_path):
    # Convert the PySpark DataFrame to a Glue DynamicFrame and write it to S3
    dyf = DynamicFrame.fromDF(spark_df, glueContext, "dyf")
    glueContext.purge_s3_path(s3_path,  {"retentionPeriod": 0})
    datasink = glueContext.write_dynamic_frame.from_options(
        frame = dyf,
        connection_type = "s3",
        connection_options = {"path": s3_path},
        format = "parquet"
    )

    print(f"Successfully Uploaded to s3 in path: {s3_path}")


def city_of_melbourne_api_request(url, api_key):
    headers = {'accept': '*/*'}
    params = {'limit': -1, 'offset': 0, 'timezone': 'UTC', 'apikey': api_key}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        print("Connection Error")
        sys.exit(1)
    
    return response