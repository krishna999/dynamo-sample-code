#!/usr/bin/env python3
import os
import json
import urllib.parse
import boto3
import ocrmypdf
import uuid
from botocore.client import Config

print('Loading function')

config = Config(retries = dict(max_attempts = 10))

def apply_ocr_to_document_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    for record in event['Records']:
        print(record)
        # Extract keys from event
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        renamed_key = key.replace('.PDF', '.pdf')
        uuidstr = str(uuid.uuid1())
        s3 = boto3.client('s3', config=config)
        try:
            inputname = '/tmp/input' + uuidstr + '.pdf'
            outputname = '/tmp/output' + uuidstr + '.pdf'
            s3.download_file(Bucket=bucket, Key=key, Filename=inputname)
            ocrmypdf.ocr(inputname, outputname, force_ocr=True, single_threaded=True)
            print('key is ', renamed_key)
            s3.upload_file(outputname, bucket, renamed_key)
            os.remove(inputname)
            os.remove(outputname)
            return
        except Exception as e:
            print(e)
            raise e
