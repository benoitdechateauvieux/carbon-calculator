import logging
import os
import json
import boto3
from enum import Enum
from urllib.parse import urlparse
from decimal import Decimal

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

EMISSION_FACTORS_TABLE_NAME = os.environ.get('EMISSIONS_FACTOR_TABLE_NAME')
INPUT_S3_BUCKET_NAME = os.environ.get('TRANSFORMED_BUCKET_NAME')
OUTPUT_S3_BUCKET_NAME = os.environ.get('ENRICHED_BUCKET_NAME')
OUTPUT_DYNAMODB_TABLE_NAME = os.environ.get('CALCULATOR_OUTPUT_TABLE_NAME')


s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

def _list_events_objects_in_s3():
    bucket = s3.Bucket(INPUT_S3_BUCKET_NAME)
    return bucket.objects.all()

def _read_events_from_s3(object_key):
    # Read activity_events object
    obj = s3.Object(INPUT_S3_BUCKET_NAME, object_key)
    activity_events_string = obj.get()['Body'].read().decode('utf-8')
    # split into individual activity_events
    activity_events = [json.loads(jline) for jline in activity_events_string.splitlines()]
    return activity_events

def _save_enriched_events_to_s3(object_key, activity_events):
    # generate the payload as string
    body = "\n".join(map(json.dumps, activity_events))
    LOGGER.info('output body: %s', "\n"+body)
    # Write to S3
    output_object_key = "today/"+object_key
    obj = s3.Object(OUTPUT_S3_BUCKET_NAME, output_object_key)
    obj.put(Body=body)
    # Return s3 URL
    return "s3://"+OUTPUT_S3_BUCKET_NAME+"/"+output_object_key

def _save_enriched_events_to_dynamodb(activity_events):
    table = dynamodb.Table(OUTPUT_DYNAMODB_TABLE_NAME)
    LOGGER.info('Saving %s activity_events in DynamoDB', len(activity_events))
    with table.batch_writer() as batch:
        for activity_event in activity_events:
            # DynamoDB expects decimals instead of floats
            activity_event_with_decimal = json.loads(json.dumps(activity_event), parse_float=Decimal)
            batch.put_item(Item=activity_event_with_decimal)

# Get emission factors from the database

# Calculate emissions

# Calculate CO2e

# Append estimated emissions to the activity event


def lambda_handler(event, context):
    LOGGER.info("Processing Emissions calculation request")
    # Paste the body of the lambda_handler here