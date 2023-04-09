import json
import logging
import boto3
import time
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a CloudWatch Logs client
cloudwatch_logs = boto3.client('logs')

def lambda_handler(event, context):
    # Extract relevant data from the event
    log_group = '/aws/batch/job'
    log_stream = 'batch-job'
    status = event['detail']['status']
    job_name = event['detail']['jobName']
    
    # Create a log stream if it doesn't exist
    try:
        response = cloudwatch_logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=log_stream
        )
        if len(response['logStreams']) == 0:
            cloudwatch_logs.create_log_stream(
                logGroupName=log_group,
                logStreamName=log_stream
            )
    except ClientError as e:
        logger.error(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error creating log stream')
        }
    
    # Write the log data to the log stream
    try:
        log_data = {
            'job_name': job_name,
            'status': status
        }
        cloudwatch_logs.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[
                {
                    'timestamp': int(round(time.time() * 1000)),
                    'message': json.dumps(log_data)
                }
            ]
        )
    except ClientError as e:
        logger.error(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error writing logs')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Logs written')
    }
