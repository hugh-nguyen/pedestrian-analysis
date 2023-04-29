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

        # # Add the AWS managed policy for Glue access to the role
        # glue_policy = iam.ManagedPolicy.from_aws_managed_policy_name(
        #     "service-role/AWSGlueServiceRole"
        # )
        # job_role.add_managed_policy(glue_policy)

        # Create an inline policy that allows passing the role to Glue jobs
        # pass_role_policy = iam.Policy(self, "glue-notebook-policy",
        #     statements=[
        #         iam.PolicyStatement(
        #             effect=iam.Effect.ALLOW,
        #             actions=["iam:PassRole"],
        #             resources=[job_role.role_arn],
        #             conditions={
        #                 "StringEquals": {
        #                     "iam:PassedToService": ["glue.amazonaws.com"]
        #                 }
        #             }
        #         )
        #     ]
        # )

        statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["iam:PassRole"],
            resources=[job_role.role_arn],
            conditions={
                "StringEquals": {
                    "iam:PassedToService": ["glue.amazonaws.com"]
                }
            }
        )
        job_role.add_to_policy(statement)


        # Create a Glue database for this work
        layers = ['raw', 'report']
        for layer in layers:
            database_name = f'pedestrian_analysis_{layer}'
            database = glue.CfnDatabase(
                self,
                database_name,
                catalog_id=os.environ.get('AWS_ACCOUNT_ID'),
                database_input={
                    'name': database_name
                }
            )

        # Create an S3 bucket to store the script file
        bucket = s3.Bucket(
            self,
            'GlueJobBucket',
            bucket_name=bucket_name,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Upload the script files to the bucket
        s3deploy.BucketDeployment(
            self, 'glue-job-scripts',
            sources=[s3deploy.Source.asset('./glue_job_scripts')],
            destination_bucket=bucket,
            destination_key_prefix='glue-job-scripts/'
        )

        # s3deploy.BucketDeployment(
        #     self, 'glue-notebooks',
        #     sources=[s3deploy.Source.asset('./glue_notebooks')],
        #     destination_bucket=bucket,
        #     destination_key_prefix='glue-notebooks/'
        # )

        for script in os.listdir('glue_job_scripts'):
            
            if script.endswith('-gluejob.py'):
                # Create a Glue Job for Sensor Count data
                job_name = script.strip('.py')
                glue.CfnJob(
                    self,
                    job_name,
                    name=job_name,
                    role=job_role.role_arn,
                    command={
                        'name': 'glueetl',
                        'pythonVersion': '3',
                        'scriptLocation': f's3://{bucket.bucket_name}/glue-job-scripts/{job_name}.py',
                    },
                    default_arguments={
                        '--job-language': 'python',
                        '--enable-continuous-cloudwatch-log': 'true',
                        '--spark-event-logs-path': 's3://aws-glue-assets-632753217422-ap-southeast-2/sparkHistoryLogs/',
                        '--enable-glue-datacatalog': 'true',
                        '--extra-py-files': f's3://{bucket_name}/glue-job-scripts/util.py',
                        '--CITY_OF_MELBOURNE_API_KEY': os.environ.get('CITY_OF_MELBOURNE_API_KEY'),
                        '--BUCKET_NAME': bucket.bucket_name
                    },
                    glue_version="3.0",
                    max_capacity=2,
                    max_retries=0,
                )

        # # Output the Glue job name and ARN
        # CfnOutput(
        #     self, 'GlueJobName',
        #     value=glue_job.ref,
        # )
        # CfnOutput(
        #     self, 'GlueJobArn',
        #     value=glue_job.attr_arn,
        # )



        # # Create a new VPC with two public subnets and two private subnets
        # vpc = ec2.Vpc(
        #     self,
        #     "pedestrian-analytics-notebook-vpc",
        #     vpc_name="pedestrian-analytics-notebook-vpc",
        #     max_azs=2,
        #     # ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16")
        #     subnet_configuration=[
        #         ec2.SubnetConfiguration(
        #             name="Public", cidr_mask=24, subnet_type=ec2.SubnetType.PUBLIC
        #         ),
        #         ec2.SubnetConfiguration(
        #             name="Private", cidr_mask=24, subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
        #         ),
        #     ],
        # )

        # Create a new subnet for the SageMaker notebook instance in the Public subnet
        # notebook_subnet = ec2.Subnet(
        #     self,
        #     "pedestrian-analytics-notebook-subnet",
        #     vpc_id=vpc.vpc_id,
        #     availability_zone=vpc.availability_zones[0],
        #     cidr_block="172.16.64.0/16",
        #     map_public_ip_on_launch=True,
        #     # subnet_type=ec2.SubnetType.PUBLIC,
        # )

        # notebook_security_group = ec2.SecurityGroup(
        #     self, 'pedestrian-analytics-notebook-security-group',
        #     vpc=vpc,
        #     allow_all_outbound=True,
        #     description='Security group for the SageMaker Notebook Instance',
        # )

        # # Create a new role
        # role = iam.CfnRole(
        #     self,
        #     'pedestrian-analytics-notebook-instance-role',
        #     role_name='pedestrian-analytics-notebook-instance-role',
        #     assume_role_policy_document={
        #         'Version': '2012-10-17',
        #         'Statement': [
        #             {
        #                 'Effect': 'Allow',
        #                 'Principal': {'Service': 'sagemaker.amazonaws.com'},
        #                 'Action': 'sts:AssumeRole'
        #             }
        #         ]
        #     },
        #     managed_policy_arns=[
        #         'arn:aws:iam::aws:policy/AmazonS3FullAccess',
        #         'arn:aws:iam::aws:policy/AmazonSageMakerFullAccess'
        #     ]
        # )

        # # cfn_code_repository = sagemaker.CfnCodeRepository(self, "MyCfnCodeRepository",
        # #     git_config=sagemaker.CfnCodeRepository.GitConfigProperty(
        # #         repository_url="https://github.com/hugh-nguyen/pedestrian-analysis-notebooks/tree/main/notebooks",
        # #         branch="main",
        # #     ),

        # #     # the properties below are optional
        # #     code_repository_name="pedestrian-analysis-notebooks",
        # # )

        # notebook_instance = sagemaker.CfnNotebookInstance(
        #     self, 'NotebookInstance',
        #     instance_type='ml.t2.medium',
        #     role_arn=role.attr_arn,
        #     notebook_instance_name='pedestrian-analytics-notebook-instance',
        #     default_code_repository='https://github.com/hugh-nguyen/pedestrian-analysis-notebooks/',
        #     direct_internet_access='Enabled',
        #     subnet_id=vpc.public_subnets[0].subnet_id,
        #     security_group_ids=[notebook_security_group.security_group_id],
        #     tags=[
        #         {
        #             'key': 'environment',
        #             'value': 'dev'
        #         }
        #     ],
        #     # lifecycle_config_name=lifecycle_config_name,
        #     # **sagemaker.CfnNotebookInstanceProps(
        #     #     # default_code_repository=default_code_repository,
        #     #     # additional_code_repositories=additional_code_repositories,
        #     #     # root_access=root_access,
        #     #     # kms_key_id=kms_key_id,
        #     #     # volume_size_in_gb=volume_size_in_gb,
        #     #     # accelerator_types=accelerator_types,
        #     #     iam_role_arn=role.attr_arn
        #     # ).to_dict()
        # )


