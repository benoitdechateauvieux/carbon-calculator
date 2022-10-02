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

def _save_enriched_events_to_redshift(activity_events):
    LOGGER.info('Saving %s activity_events in Redshift', len(activity_events))
    for activity_event in activity_events:
        redshift.execute_statement(
            Database=redshift_db_name,
            SecretArn=REDSHIFT_SECRET,
            Sql= "INSERT INTO calculated_emissions(activity_event_id, asset_id, geo, origin_measurement_timestamp, scope, category, activity, source, raw_data, units, co2e_amount, co2e_unit, n2o_amount, n2o_unit, ch4_amount, ch4_unit, co2_amount, co2_unit, emissions_factor_amount, emissions_factor_unit) VALUES (:activity_event_id, :asset_id, ST_Point(:long, :lat), :origin_measurement_timestamp, :scope, :category, :activity, :source, :raw_data, :units, :co2e_amount, :co2e_unit, :n2o_amount, :n2o_unit, :ch4_amount, :ch4_unit, :co2_amount, :co2_unit, :emissions_factor_amount, :emissions_factor_unit)",
            ClusterIdentifier=redshift_cluster_identifier,
            Parameters=[
                {'name': 'activity_event_id', 'value': activity_event['activity_event_id']},
                {'name': 'asset_id', 'value': activity_event.get('asset_id',' ')},
                {'name': 'long', 'value': str(activity_event['geo']['long']) if activity_event.get('geo', None) else '0.0'}, #default value only for the simplicity of the workshop
                {'name': 'lat', 'value': str(activity_event['geo']['lat']) if activity_event.get('geo', None) else '0.0'}, #default value only for the simplicity of the workshop
                {'name': 'origin_measurement_timestamp', 'value': activity_event.get('origin_measurement_timestamp','2022-06-26 02:31:29')}, #default value only for the simplicity of the workshop
                {'name': 'scope', 'value': str(activity_event['scope'])},
                {'name': 'category', 'value': activity_event['category']},
                {'name': 'activity', 'value': activity_event['activity']},
                {'name': 'source', 'value': activity_event.get('source',' ')},
                {'name': 'raw_data', 'value': str(activity_event['raw_data'])},
                {'name': 'units', 'value': activity_event['units']},
                {'name': 'co2e_amount', 'value': str(activity_event['emissions_output']['calculated_emissions']['co2e']['amount'])},
                {'name': 'co2e_unit', 'value': activity_event['emissions_output']['calculated_emissions']['co2e']['unit']},
                {'name': 'n2o_amount', 'value': str(activity_event['emissions_output']['calculated_emissions']['n2o']['amount'])},
                {'name': 'n2o_unit', 'value': activity_event['emissions_output']['calculated_emissions']['n2o']['unit']},
                {'name': 'ch4_amount', 'value': str(activity_event['emissions_output']['calculated_emissions']['ch4']['amount'])},
                {'name': 'ch4_unit', 'value': activity_event['emissions_output']['calculated_emissions']['ch4']['unit']},
                {'name': 'co2_amount', 'value': str(activity_event['emissions_output']['calculated_emissions']['co2']['amount'])},
                {'name': 'co2_unit', 'value': activity_event['emissions_output']['calculated_emissions']['co2']['unit']},
                {'name': 'emissions_factor_amount', 'value': str(activity_event['emissions_output']['emissions_factor']['amount'])},
                {'name': 'emissions_factor_unit', 'value': activity_event['emissions_output']['emissions_factor']['unit']}
            ]
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