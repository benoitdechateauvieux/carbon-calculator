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


def _get_emissions_factor(activity, category):
    cache_key = frozenset({activity, category})
    if cache_key in emission_factors_cache:
        return emission_factors_cache[cache_key]
    else:
        table = dynamodb.Table(EMISSION_FACTORS_TABLE_NAME)
        coefficient = table.get_item(
            Key={
                'category': category,
                'activity': activity,
            }
        )
        emission_factors_cache[cache_key] = coefficient
        return coefficient


def _calculate_emission(raw_data, factor):
    return float(raw_data) * float(0 if factor == '' else factor) / 1000

class Gas(Enum):
    CO2 = 1  # Carbon dioxide
    CH4 = 2  # Methane
    N2O = 3  # Nitrous oxide
    NF3 = 4  # Nitrogen trifluoride
    SF6 = 5  # Sulfur hexafluoride


# Global warming potential (from IPCC report AR5)
GWP = {
    Gas.CO2: 1,
    Gas.CH4: 28,
    Gas.N2O: 265,
    Gas.NF3: 16100,
    Gas.SF6: 23500,
}


def _calculate_co2e(co2_emissions, ch4_emissions, n2o_emissions):
    result = co2_emissions * GWP[Gas.CO2]
    result += ch4_emissions * GWP[Gas.CH4]
    result += n2o_emissions * GWP[Gas.N2O]
    return result

def _append_emissions(activity_event):
    emissions_factor = _get_emissions_factor(activity_event['activity'], activity_event['category'])
    coefficients = emissions_factor['Item']['emissions_factor_standards']['ghg']['coefficients']

    raw_data = activity_event['raw_data']
    co2_emissions = _calculate_emission(raw_data, coefficients['co2_factor'])
    ch4_emissions = _calculate_emission(raw_data, coefficients['ch4_factor'])
    n2o_emissions = _calculate_emission(raw_data, coefficients['n2o_factor'])
    co2e_emissions = _calculate_co2e(co2_emissions, ch4_emissions, n2o_emissions)
    emissions_output = {
        "emissions_output": {
            "calculated_emissions": {
                "co2": {
                    "amount": co2_emissions,
                    "unit": "tonnes"
                },
                "ch4": {
                    "amount": ch4_emissions,
                    "unit": "tonnes"
                },
                "n2o": {
                    "amount": n2o_emissions,
                    "unit": "tonnes"
                },
                "co2e": {
                    "amount": co2e_emissions,
                    "unit": "tonnes"
                }
            },
            "emissions_factor": {
                "amount": float(coefficients['AR5_kgco2e']),
                "unit": "kgCO2e/unit"
            }
        }
    }
    activity_event.update(emissions_output)
    return activity_event


def lambda_handler(event, context):
    for event_object in _list_events_objects_in_s3():
        activity_events = _read_events_from_s3(event_object)
        activity_events_with_emissions = list(map(_append_emissions, activity_events))
        _save_enriched_events_to_redshift(event_object, activity_events_with_emissions)
        _save_enriched_events_to_dynamodb(activity_events_with_emissions)
