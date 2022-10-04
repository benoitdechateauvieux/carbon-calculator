import logging
import os
import json
import boto3
from enum import Enum
from urllib.parse import urlparse
from decimal import Decimal

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

S3_PREFIXES = ["scope1-cleansed-data", "scope2-bill-extracted-data"]

EMISSION_FACTORS_TABLE_NAME = os.environ.get('EMISSIONS_FACTOR_TABLE_NAME')
INPUT_S3_BUCKET_NAME = os.environ.get('TRANSFORMED_BUCKET_NAME')
REDSHIFT_SECRET = os.environ.get('REDSHIFT_SECRET')
OUTPUT_S3_BUCKET_NAME = os.environ.get('OUTPUT_S3_BUCKET_NAME')
REDSHIFT_ROLE_ARN = os.environ.get('REDSHIFT_ROLE_ARN')
OUTPUT_DYNAMODB_TABLE_NAME = os.environ.get('CALCULATOR_OUTPUT_TABLE_NAME')

dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')
s3client = boto3.client("s3")
secretsmanager = boto3.client('secretsmanager')
redshift = boto3.client('redshift-data')

# Get Redshift cluster and database names
secret_value = secretsmanager.get_secret_value(SecretId=REDSHIFT_SECRET)
secret = secret_value['SecretString']
secret_json = json.loads(secret)
redshift_db_name = secret_json['dbname']
redshift_cluster_identifier = secret_json['dbClusterIdentifier']

emission_factors_cache = {}

def _list_events_objects_in_s3():
    objects = []
    for prefix in S3_PREFIXES:
        response = s3client.list_objects_v2(
            Bucket = INPUT_S3_BUCKET_NAME,
            Prefix = prefix
        )
        if response['KeyCount'] > 0:
            objects += map(lambda object: object['Key'], response['Contents'])
    return objects

def _read_events_from_s3(object_key):
    # Read activity_events object
    obj = s3.Object(INPUT_S3_BUCKET_NAME, object_key)
    activity_events_string = obj.get()['Body'].read().decode('utf-8')
    # split into individual activity_events
    activity_events = [json.loads(jline) for jline in activity_events_string.splitlines()]
    return activity_events

def _save_enriched_events_to_redshift(object_key, activity_events):
    LOGGER.info('Saving %s activity_events in Redshift', len(activity_events))
    # generate the payload as string
    csv_body = ""
    for activity_event in activity_events:
        csv_body += activity_event['activity_event_id']
        csv_body +=","+activity_event.get('asset_id','')
        csv_body +=","+(str(json.loads(activity_event['geo'])[0]) if activity_event.get('geo',None) else '')
        csv_body +=","+(str(json.loads(activity_event['geo'])[1]) if activity_event.get('geo',None) else '')
        csv_body +=","+activity_event.get('origin_measurement_timestamp','')
        csv_body +=","+str(activity_event['scope'])
        csv_body +=",\""+activity_event['category']+"\""
        csv_body +=",\""+activity_event['activity']+"\""
        csv_body +=","+activity_event.get('source','')
        csv_body +=","+str(activity_event['raw_data'])
        csv_body +=","+activity_event['units']
        csv_body +=","+str(activity_event['emissions_output']['calculated_emissions']['co2e']['amount'])
        csv_body +=","+activity_event['emissions_output']['calculated_emissions']['co2e']['unit']
        csv_body +=","+str(activity_event['emissions_output']['calculated_emissions']['n2o']['amount'])
        csv_body +=","+activity_event['emissions_output']['calculated_emissions']['n2o']['unit']
        csv_body +=","+str(activity_event['emissions_output']['calculated_emissions']['ch4']['amount'])
        csv_body +=","+activity_event['emissions_output']['calculated_emissions']['ch4']['unit']
        csv_body +=","+str(activity_event['emissions_output']['calculated_emissions']['co2']['amount'])
        csv_body +=","+activity_event['emissions_output']['calculated_emissions']['co2']['unit']
        csv_body +=","+str(activity_event['emissions_output']['emissions_factor']['amount'])
        csv_body +=","+activity_event['emissions_output']['emissions_factor']['unit']
        csv_body +="\n"
    # Write to S3
    output_object_key = object_key.replace(".json", ".csv")
    obj = s3.Object(OUTPUT_S3_BUCKET_NAME, output_object_key)
    obj.put(Body=csv_body)
    # Copy to Redshift
    object_url =  "s3://"+OUTPUT_S3_BUCKET_NAME+"/"+output_object_key
    sql = "COPY calculated_emissions FROM '"+object_url+"' IAM_ROLE '"+REDSHIFT_ROLE_ARN+"' CSV TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS';"
    resp = redshift.execute_statement(
        Database=redshift_db_name,
        SecretArn=REDSHIFT_SECRET,
        ClusterIdentifier=redshift_cluster_identifier,
        Sql=sql
    )


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