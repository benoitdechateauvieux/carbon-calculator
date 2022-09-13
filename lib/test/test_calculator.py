import logging
import json
import boto3
import os
from emission_output import EmissionOutput

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')

INPUT_BUCKET_NAME = os.environ['INPUT_BUCKET_NAME']
OUTPUT_BUCKET_NAME = os.environ['OUTPUT_BUCKET_NAME']
CALCULATOR_FUNCTION_NAME = os.environ['CALCULATOR_FUNCTION_NAME']

def test_scope1():
    _test_calculator([
        {
            "key": "testscope1.json",
            "body": b'{"activity_event_id": "customer-carbonlake-12345", "asset_id": "vehicle-1234", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 103.45, "units": "gal"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 1.0562245000000001, 1.1638125000000002e-06, 2.3276250000000004e-06, 1.0569472275625, 1.056873907375, 10.21698625, 10.2162775)]
        }
    ])

def test_scope1_2lines():
    _test_calculator([
        {
            "key": "testscope1_2lines.json",
            "body": b'''{"activity_event_id": "customer-carbonlake-12345", "asset_id": "vehicle-1234", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 103.45, "units": "gal"}
                        {"activity_event_id": "customer-carbonlake-12346", "asset_id": "vehicle-1235", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 13.5, "units": "gal"}''',
            "expected": [EmissionOutput("customer-carbonlake-12345", 1.0562245000000001, 1.1638125000000002e-06, 2.3276250000000004e-06, 1.0569472275625, 1.056873907375, 10.21698625, 10.2162775),
                         EmissionOutput("customer-carbonlake-12346", 0.137835, 1.5187500000000003e-07, 3.0375000000000006e-07, 0.137929314375, 0.13791974625, 10.21698625, 10.2162775)]
        }
    ])

def test_scope2_location_based():
    _test_calculator([
        {
            "key": "testscope2_location.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "grid-region-location-based", "activity": "Quebec", "raw_data": 453, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.0005436, 0.0, 4.5299999999999995e-08, 0.0005570994, 0.0005556045, 0.0012298, 0.0012265)]
        }
    ])

def test_scope2_market_based_residual_mix():
    _test_calculator([
        {
            "key": "testscope2_market.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "egrid-subregion-residual-mix-market-based", "activity": "Quebec", "raw_data": 453, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.020414174106, 0.0, 0.0, 0.020414174106, 0.020414174106, 0.045064402, 0.045064402)]
        }
    ])

def test_multiple_events_objects():
    _test_calculator([
        {
            "key": "testscope2_location.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "grid-region-location-based", "activity": "Quebec", "raw_data": 453, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.0005436, 0.0, 4.5299999999999995e-08, 0.0005570994, 0.0005556045, 0.0012298, 0.0012265)]
        },
        {
            "key": "testscope2_market.json",
            "body": b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "egrid-subregion-residual-mix-market-based", "activity": "Quebec", "raw_data": 453, "units": "kwH"}',
            "expected": [EmissionOutput("customer-carbonlake-12345", 0.020414174106, 0.0, 0.0, 0.020414174106, 0.020414174106, 0.045064402, 0.045064402)]
        }
    ])

def _test_calculator(events_objects):
    try:
        # Given
        for events_object in events_objects:
            s3 = boto3.resource('s3')
            s3.Object(INPUT_BUCKET_NAME, events_object['key']).put(Body=events_object['body'])
        # When
        client = boto3.client('lambda')
        client.invoke(
            FunctionName=CALCULATOR_FUNCTION_NAME,
        )
        # Then
        for events_object in events_objects:
            print("Testing ", events_object['key'])
            # Get output object
            outputKey = "today/"+events_object['key']
            outputObject = s3.Object(OUTPUT_BUCKET_NAME, outputKey).get()
            outputBody = outputObject['Body'].read().decode("utf-8")
            # Test output object
            outputActivityEvents = [json.loads(jline) for jline in outputBody.splitlines()]
            expected_emissions = events_object['expected']
            for index in range(len(expected_emissions)):
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2']['amount'] == expected_emissions[index].co2
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2']['unit'] == 'tonnes'
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['ch4']['amount'] == expected_emissions[index].ch4
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['ch4']['unit'] == 'tonnes'
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['n2o']['amount'] == expected_emissions[index].n2o
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['n2o']['unit'] == 'tonnes'
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2e']['amount'] == expected_emissions[index].co2e_ar5
                assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2e']['unit'] == 'tonnes'
                assert outputActivityEvents[index]['emissions_output']['emissions_factor']['amount'] == expected_emissions[index].emissions_factor_ar5
                assert outputActivityEvents[index]['emissions_output']['emissions_factor']['unit'] == 'kgCO2e/unit'
    finally:
        # Cleanup
        for events_object in events_objects:
            s3.Object(INPUT_BUCKET_NAME, events_object['key']).delete()
            s3.Object(OUTPUT_BUCKET_NAME, "today/"+events_object['key']).delete()

def lambda_handler(event, context):
  logger.info('EVENT:')
  logger.info(json.dumps(event, indent=4, sort_keys=True))

  test_scope1()
  test_scope1_2lines()
  test_scope2_location_based()
  test_scope2_market_based_residual_mix()
  test_multiple_events_objects()

  return 'Success'