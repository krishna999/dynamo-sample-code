import boto3
import json
import os
import urllib.parse
from urllib.parse import unquote
from botocore.client import Config


SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
TEXTRACT_ROLE = os.environ['TEXTRACT_ROLE']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))

    # Set directory for preprocessing PDFs
    os.chdir('/tmp/')
    for record in event['requestPayload']['Records']:
        print(record)
        # Extract keys from event
        bucket = record['s3']['bucket']['name']
        urlKey = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
    
        # Start textract job with new file
        client = getClient('textract')
        response = client.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': urlKey
                }
            },
            FeatureTypes=[
                'TABLES','FORMS'
            ],
            # ClientRequestToken=reqId,
            JobTag='HPtextract',
            NotificationChannel={
                'SNSTopicArn': SNS_TOPIC_ARN,
                'RoleArn': TEXTRACT_ROLE
            },
            OutputConfig={
                'S3Bucket': OUTPUT_BUCKET,
                'S3Prefix': urlKey.split('.')[0]
            }
        )
        print(response)
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }



def getClient(name, awsRegion=None):
    config = Config(
        retries = dict(
            max_attempts = 30
        )
    )
    if(awsRegion):
        return boto3.client(name, region_name=awsRegion, config=config)
    else:
        return boto3.client(name, config=config) 
            
def getResource(name, awsRegion=None):
    config = Config(
        retries = dict(
            max_attempts = 30
        )
    )

    if(awsRegion):
        return boto3.resource(name, region_name=awsRegion, config=config)
    else:
        return boto3.resource(name, config=config)
