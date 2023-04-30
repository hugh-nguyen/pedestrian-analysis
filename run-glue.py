import boto3, os, time, sys

# Initialize the Glue client
glue = boto3.client('glue')
bucket_name = os.environ.get('BUCKET_NAME')

for script in os.listdir('glue_job_scripts'):
            
    if script.endswith('-gluejob.py'):
        
        print(script)
        job_name = script.strip('.py')

        arguments = {
            '--job-language': 'python',
            '--enable-continuous-cloudwatch-log': 'true',
            '--spark-event-logs-path': 's3://aws-glue-assets-632753217422-ap-southeast-2/sparkHistoryLogs/',
            '--enable-glue-datacatalog': 'true',
            '--extra-py-files': f's3://{bucket_name}/glue-job-scripts/util.py',
            '--CITY_OF_MELBOURNE_API_KEY': os.environ.get('CITY_OF_MELBOURNE_API_KEY'),
            '--BUCKET_NAME': bucket_name
        }

        # Start the job
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=arguments
        )

        # Get the job run ID
        job_run_id = response['JobRunId']

        # Wait for the job to finish
        while True:
            # Get the status of the job run
            status = glue.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
            
            if status == 'SUCCEEDED':
                # Job completed successfully
                print('Job completed successfully')
                break
            elif status in ['FAILED', 'STOPPED']:
                # Job failed or was stopped
                print('Job failed or was stopped')
                sys.exit(1)
            else:
                # Job is still running
                print('Job is still running...')
                time.sleep(20) # wait for 30 seconds before checking status again
