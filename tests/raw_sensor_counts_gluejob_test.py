import pytest, os
import sys; print(sys.path)
sys.path.append(os.getcwd())
sys.path.append(os.getcwd()+'/glue_job_scripts')
from glue_job_scripts import raw_sensor_counts_gluejob

def test_create_glue_catalog_table():
    assert city_of_melbourne_pedestrian_counts.create_glue_catalog_table(
        'test_database', 'test_table', city_of_melbourne_pedestrian_counts.schema, 's3://test_bucket/test_path'
    ) == None


def test_city_of_melbourne_api_request():
    response = city_of_melbourne_pedestrian_counts.city_of_melbourne_api_request(
        'https://melbournetestbed.opendatasoft.com/api/v2/catalog/datasets/pedestrian-counting-system-monthly-counts-per-hour/attachments',
        'test_api_key'
    )
    assert response.status_code == 200


def test_upload_to_s3():
    with pytest.raises(Exception) as e:
        city_of_melbourne_pedestrian_counts.upload_to_s3(None, None, 'invalid_path')
    assert str(e.value) == 'Invalid S3 Path: invalid_path'

    with pytest.raises(Exception) as e:
        city_of_melbourne_pedestrian_counts.upload_to_s3(None, None, 's3://test_bucket/test_path')
    assert str(e.value) == 'No Spark Dataframe provided'

    assert city_of_melbourne_pedestrian_counts.upload_to_s3(
        None,
        city_of_melbourne_pedestrian_counts.spark.createDataFrame(
            [(1, '2022-05-01T00:00:00', 1, 'sensor_name', 10)],
            schema=city_of_melbourne_pedestrian_counts.schema
        ),
        's3://test_bucket/test_path'
    ) == None


def test_city_of_melbourne_pedestrian_counts():
    with pytest.raises(Exception) as e:
        city_of_melbourne_pedestrian_counts.city_of_melbourne_pedestrian_counts({
            'JOB_NAME': 'test_job',
            'CITY_OF_MELBOURNE_API_KEY': 'test_api_key',
            'BUCKET_NAME': 'test_bucket',
        })
    assert str(e.value) == 'No Spark Dataframe provided'

    assert city_of_melbourne_pedestrian_counts.city_of_melbourne_pedestrian_counts({
        'JOB_NAME': 'test_job',
        'CITY_OF_MELBOURNE_API_KEY': 'test_api_key',
        'BUCKET_NAME': 'test_bucket',
    }, city_of_melbourne_pedestrian_counts.spark.createDataFrame(
        [(1, '2022-05-01T00:00:00', 1, 'sensor_name', 10)],
        schema=city_of_melbourne_pedestrian_counts.schema
    )) == None
