import logging
import json
import boto3
from botocore.exceptions import WaiterError
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client
import os
from emission_output import EmissionOutput
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
secretsmanager = boto3.client('secretsmanager')
redshift = boto3.client('redshift-data')

waiter_name = 'DataAPIExecution'
waiter_config = {
  'version': 2,
  'waiters': {
    'DataAPIExecution': {
      'operation': 'DescribeStatement',
      'delay': 1,
      'maxAttempts': 3,
      'acceptors': [
        {
          "matcher": "path",
          "expected": "FINISHED",
          "argument": "Status",
          "state": "success"
        },
        {
          "matcher": "pathAny",
          "expected": ["PICKED","STARTED","SUBMITTED"],
          "argument": "Status",
          "state": "retry"
        },
        {
          "matcher": "pathAny",
          "expected": ["FAILED","ABORTED"],
          "argument": "Status",
          "state": "failure"
        }
      ],
    },
  },
}
waiter_model = WaiterModel(waiter_config)
custom_waiter = create_waiter_with_client(waiter_name, waiter_model, redshift)


INPUT_BUCKET_NAME = os.environ['INPUT_BUCKET_NAME']
REDSHIFT_SECRET = os.environ.get('REDSHIFT_SECRET')
CALCULATOR_FUNCTION_NAME = os.environ['CALCULATOR_FUNCTION_NAME']

def test_scope1():
    _test_calculator([
        {
            "key": "scope1-cleansed-data/testscope1.json",
            "body": b'{"activity_event_id": "customer-carbonlake-12345", "asset_id": "vehicle-1234", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 103.45, "units": "gal"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 1.0562245000000001, 1.1638125e-06, 2.327625e-06, 1.0569472275625, 1.056873907375, 10.21698625, 10.2162775)]
        }
    ])

def test_scope1_2lines():
    _test_calculator([
        {
            "key": "scope1-cleansed-data/testscope1_2lines.json",
            "body": b'''{"activity_event_id": "customer-carbonlake-12345", "asset_id": "vehicle-1234", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 103.46, "units": "gal"}
                        {"activity_event_id": "customer-carbonlake-12346", "asset_id": "vehicle-1235", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 13.5, "units": "gal"}''',
            "expected": [EmissionOutput("customer-carbonlake-12345", 1.0563266, 1.1639249999e-06, 2.3278499999e-06, 1.0569472275625, 1.05697607015, 10.21698625, 10.2162775),
                         EmissionOutput("customer-carbonlake-12346", 0.137835, 1.51875e-07, 3.0375e-07, 0.137929314375, 0.13791974625, 10.21698625, 10.2162775)]
        }
    ])

def test_scope2_location_based():
    _test_calculator([
        {
            "key": "scope2-bill-extracted-data/testscope2_location.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "grid-region-location-based", "activity": "Quebec", "raw_data": 453, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.0005436, 0.0, 4.52999999e-08, 0.0005570994, 0.0005556045, 0.0012298, 0.0012265)]
        }
    ])

def test_scope2_market_based_residual_mix():
    _test_calculator([
        {
            "key": "scope2-bill-extracted-data/testscope2_market.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "egrid-subregion-residual-mix-market-based", "activity": "Quebec", "raw_data": 454, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.020459238508, 0.0, 0.0, 0.020459238508, 0.020459238508, 0.045064402, 0.045064402)]
        }
    ])

def test_multiple_events_objects():
    _test_calculator([
        {
            "key": "scope2-bill-extracted-data/testscope2_location.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "grid-region-location-based", "activity": "Quebec", "raw_data": 455, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.0005459999999999, 0.0, 4.55e-08, 0.0005570994, 0.0005580574999999, 0.0012298, 0.0012265)]
        },
        {
            "key": "scope2-bill-extracted-data/testscope2_market.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "egrid-subregion-residual-mix-market-based", "activity": "Quebec", "raw_data": 456, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.0005580574999999, 0.0, 0.0, 0.0005580574999999, 0.0005580574999999, 0.045064402, 0.045064402)]
        }
    ])

def _test_calculator(events_objects):
    # get DB infos
    secret_value = secretsmanager.get_secret_value(SecretId=REDSHIFT_SECRET)
    secret = secret_value['SecretString']
    secret_json = json.loads(secret)
    database = secret_json['dbname']
    cluster_id = secret_json['dbClusterIdentifier']

    # Cleanup
    truncate_resp=redshift.execute_statement(
        Database=database,
        SecretArn=REDSHIFT_SECRET,
        Sql= "TRUNCATE TABLE calculated_emissions",
        ClusterIdentifier=cluster_id,
    )
    custom_waiter.wait(Id=truncate_resp['Id'])

    try:
        # Given
        for events_object in events_objects:
            s3 = boto3.resource('s3')
            s3.Object(INPUT_BUCKET_NAME, events_object['key']).put(Body=events_object['body'])
        # When
        client = boto3.client('lambda')
        client.invoke(FunctionName=CALCULATOR_FUNCTION_NAME)
        # Then
        for events_object in events_objects:
            print("Testing ", events_object['key'])
            # Get rows
            response = redshift.execute_statement(
                Database=database,
                SecretArn=REDSHIFT_SECRET,
                Sql= "SELECT * FROM calculated_emissions",
                ClusterIdentifier=cluster_id,
            )
            statement_id=response['Id']
            custom_waiter.wait(Id=statement_id)
            response = redshift.get_statement_result(Id=statement_id)
            # # Test calculated emissions
            expected_emissions = events_object['expected']
            actual_emissions = response['Records']
            assert len(expected_emissions) == len(actual_emissions)
            for index in range(len(expected_emissions)):
                logger.info(float(actual_emissions[index][10]['stringValue']))
                logger.info(expected_emissions[index].co2e_ar5)
                logger.info(float(actual_emissions[index][12]['stringValue']))
                logger.info(expected_emissions[index].n2o)
                logger.info(float(actual_emissions[index][14]['stringValue']))
                logger.info(expected_emissions[index].ch4)
                logger.info(float(actual_emissions[index][16]['stringValue']))
                logger.info(expected_emissions[index].co2)
                logger.info(float(actual_emissions[index][18]['stringValue']))
                logger.info(expected_emissions[index].emissions_factor_ar5)

                assert float(actual_emissions[index][10]['stringValue']) == expected_emissions[index].co2e_ar5
                assert actual_emissions[index][11]['stringValue'] == 'tonnes'
                assert float(actual_emissions[index][12]['stringValue']) == expected_emissions[index].n2o
                assert actual_emissions[index][13]['stringValue'] == 'tonnes'
                assert float(actual_emissions[index][14]['stringValue']) == expected_emissions[index].ch4
                assert actual_emissions[index][15]['stringValue'] == 'tonnes'
                assert float(actual_emissions[index][16]['stringValue']) == expected_emissions[index].co2
                assert actual_emissions[index][17]['stringValue'] == 'tonnes'
                assert float(actual_emissions[index][18]['stringValue']) == expected_emissions[index].emissions_factor_ar5
                assert actual_emissions[index][19]['stringValue'] == 'kgCO2e/unit'
            # Cleanup
            truncate_resp=redshift.execute_statement(
                Database=database,
                SecretArn=REDSHIFT_SECRET,
                Sql= "TRUNCATE TABLE calculated_emissions",
                ClusterIdentifier=cluster_id,
            )
            custom_waiter.wait(Id=truncate_resp['Id'])
    finally:
        # Cleanup
        for events_object in events_objects:
            s3.Object(INPUT_BUCKET_NAME, events_object['key']).delete()

def lambda_handler(event, context):
  logger.info('EVENT:')
  logger.info(json.dumps(event, indent=4, sort_keys=True))

  test_scope1()
  test_scope1_2lines()
  test_scope2_location_based()
  test_scope2_market_based_residual_mix()
  test_multiple_events_objects()

  return 'Success'