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
    inputKey = "testscope1.json"
    inputBody = b'{"activity_event_id": "customer-carbonlake-12345", "asset_id": "vehicle-1234", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 103.45, "units": "gal"}'
    _test_calculator(inputKey, inputBody, [EmissionOutput("customer-carbonlake-12345", 1.0562245000000001, 1.1638125000000002e-06, 2.3276250000000004e-06, 1.0569472275625, 1.056873907375, 10.21698625, 10.2162775)])

def test_scope1_2lines():
    inputKey = "testscope1_2lines.json"
    inputBody = b'''{"activity_event_id": "customer-carbonlake-12345", "asset_id": "vehicle-1234", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 103.45, "units": "gal"}
    {"activity_event_id": "customer-carbonlake-12346", "asset_id": "vehicle-1235", "geo": { "lat": 45.5152, "long": 122.6784}, "origin_measurement_timestamp":"2022-06-26 02:31:29", "scope": 1, "category": "mobile-combustion", "activity": "Diesel Fuel - Diesel Passenger Cars", "source": "company_fleet_management_database", "raw_data": 13.5, "units": "gal"}'''
    _test_calculator(inputKey, inputBody, [EmissionOutput("customer-carbonlake-12345", 1.0562245000000001, 1.1638125000000002e-06, 2.3276250000000004e-06, 1.0569472275625, 1.056873907375, 10.21698625, 10.2162775),
                                                    EmissionOutput("customer-carbonlake-12346", 0.137835, 1.5187500000000003e-07, 3.0375000000000006e-07, 0.137929314375, 0.13791974625, 10.21698625, 10.2162775)])

def test_scope2_location_based():
    inputKey = "testscope2_location.json"
    inputBody = b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "grid-region-location-based", "activity": "Quebec", "raw_data": 453, "units": "kwH"}'
    _test_calculator(inputKey, inputBody, [EmissionOutput("customer-carbonlake-12345", 0.0005436, 0.0, 4.5299999999999995e-08, 0.0005570994, 0.0005556045, 0.0012298, 0.0012265)])

def test_scope2_market_based_residual_mix():
    inputKey = "testscope2_market.json"
    inputBody = b'{ "activity_event_id": "customer-carbonlake-12345", "supplier": "eversource", "scope": 2, "category": "egrid-subregion-residual-mix-market-based", "activity": "Quebec", "raw_data": 453, "units": "kwH"}'
    _test_calculator(inputKey, inputBody, [EmissionOutput("customer-carbonlake-12345", 0.020414174106, 0.0, 0.0, 0.020414174106, 0.020414174106, 0.045064402, 0.045064402)])

def _test_calculator(inputKey, inputBody, emissionOutputs):
    print("Testing ", inputKey)
    try:
        # Given
        s3 = boto3.resource('s3')
        s3.Object(INPUT_BUCKET_NAME, inputKey).put(Body=inputBody)
        # When
        client = boto3.client('lambda')
        payload = '{"storage_location": "s3://'+INPUT_BUCKET_NAME+'/'+inputKey+'" }'
        response = client.invoke(
            FunctionName=CALCULATOR_FUNCTION_NAME,
            Payload=payload,
        )
        # Then
        outputKey = "today/"+inputKey
        outputObject = s3.Object(OUTPUT_BUCKET_NAME, outputKey).get()
        outputBody = outputObject['Body'].read().decode("utf-8")
        outputActivityEvents = [json.loads(jline) for jline in outputBody.splitlines()]
        for index in range(len(emissionOutputs)):
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2']['amount'] == emissionOutputs[index].co2
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2']['unit'] == 'tonnes'
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['ch4']['amount'] == emissionOutputs[index].ch4
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['ch4']['unit'] == 'tonnes'
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['n2o']['amount'] == emissionOutputs[index].n2o
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['n2o']['unit'] == 'tonnes'
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2e']['amount'] == emissionOutputs[index].co2e_ar5
            assert outputActivityEvents[index]['emissions_output']['calculated_emissions']['co2e']['unit'] == 'tonnes'
            assert outputActivityEvents[index]['emissions_output']['emissions_factor']['amount'] == emissionOutputs[index].emissions_factor_ar5
            assert outputActivityEvents[index]['emissions_output']['emissions_factor']['unit'] == 'kgCO2e/unit'
    finally:
        # Cleanup
        s3.Object(INPUT_BUCKET_NAME, inputKey).delete()
        s3.Object(OUTPUT_BUCKET_NAME, outputKey).delete()

def lambda_handler(event, context):
  logger.info('EVENT:')
  logger.info(json.dumps(event, indent=4, sort_keys=True))

  test_scope1()
  test_scope1_2lines()
  test_scope2_location_based()
  test_scope2_market_based_residual_mix()

  return 'Success'