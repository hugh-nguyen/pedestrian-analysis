from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_s3_deployment as s3deploy,
    aws_sagemaker as sagemaker,
    Stack,
    RemovalPolicy
)
import uuid

from constructs import Construct
import os


class PedestrianAnalysisStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create a bucket name with a UUID that is almost certainly globally unqiue
        bucket_name = os.environ.get('BUCKET_NAME')

        # Create an IAM role for the Glue job
        job_role = iam.Role(
            self, 'GlueJobRole',
            role_name='pedestrians-analysis-gluejob-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceNotebookRole'),
            ],
        )

        # statement = iam.PolicyStatement(
        #     effect=iam.Effect.ALLOW,
        #     actions=["iam:PassRole"],
        #     resources=[job_role.role_arn],
        #     conditions={
        #         "StringEquals": {
        #             "iam:PassedToService": ["glue.amazonaws.com"]
        #         }
        #     }
        # )
        # job_role.add_to_policy(statement)


        # Create a Glue database for this work
        layers = ['raw', 'report']
        for layer in layers:
            database_name = f'pedestrian_analysis_{layer}'
            database = glue.CfnDatabase(
                self,
                database_name,
                catalog_id=os.environ.get('CDK_DEPLOY_ACCOUNT'),
                database_input={
                    'name': database_name
                }
            )

        # # Create an S3 bucket to store the script file
        # bucket = s3.Bucket(
        #     self,
        #     'GlueJobBucket',
        #     bucket_name=bucket_name,
        #     removal_policy=RemovalPolicy.DESTROY
        # )

        bucket = s3.Bucket.from_bucket_name(
            self,
            "working-bucket",
            bucket_name
        )

        # Upload the script files to the bucket
        s3deploy.BucketDeployment(
            self, 'glue-job-scripts',
            sources=[s3deploy.Source.asset('./glue_job_scripts')],
            destination_bucket=bucket,
            destination_key_prefix='glue-job-scripts/'
        )

        for script in os.listdir('glue_job_scripts'):
            
            if script.endswith('-gluejob.py'):

                job_name = script.strip('.py')
                glue.CfnJob(
                    self,
                    job_name,
                    name=job_name,
                    role=job_role.role_arn,
                    command={
                        'name': 'glueetl',
                        'pythonVersion': '3',
                        'scriptLocation': f's3://{bucket_name}/glue-job-scripts/{job_name}.py',
                    },
                    default_arguments={
                        '--job-language': 'python',
                        '--enable-continuous-cloudwatch-log': 'true',
                        '--spark-event-logs-path': 's3://aws-glue-assets-632753217422-ap-southeast-2/sparkHistoryLogs/',
                        '--enable-glue-datacatalog': 'true',
                        '--extra-py-files': f's3://{bucket_name}/glue-job-scripts/util.py',
                        '--CITY_OF_MELBOURNE_API_KEY': os.environ.get('CITY_OF_MELBOURNE_API_KEY'),
                        '--BUCKET_NAME': bucket_name
                    },
                    glue_version="3.0",
                    max_capacity=2,
                    max_retries=0,
                )
