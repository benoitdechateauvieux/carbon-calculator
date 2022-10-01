import { Duration, Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as redshift from '@aws-cdk/aws-redshift-alpha';
import { custom_resources as cr } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as path from 'path';
import emission_factors from './emissions_factor_model_2022-05-22.json';

const DDB_BATCH_WRITE_ITEM_CHUNK_SIZE = 25;
const REDSHIFT_DB_NAME = "emissions";

export class CarbonCalculatorLambdaStack extends Stack {
  public readonly calculatorOutputTable: dynamodb.Table;
  public readonly inputBucket: s3.Bucket;
  public readonly outputCluster: redshift.Cluster;
  public readonly calculatorFunction: lambda.Function;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'carbon-calculator-vpc');

    this.inputBucket = new s3.Bucket(this, 'CarbonCalculatorInputBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    this.outputCluster = new redshift.Cluster(this, 'CarbonCalculatorRedshiftCluster', {
      masterUser: {
        masterUsername: "admin"
      },
      clusterType: redshift.ClusterType.SINGLE_NODE,
      defaultDatabaseName: REDSHIFT_DB_NAME,
      removalPolicy: RemovalPolicy.DESTROY,
      vpc
    });
    const outputTable = new redshift.Table(this, 'CarbonCalculatorRedshiftTable', {
      cluster: this.outputCluster,
      databaseName: REDSHIFT_DB_NAME,
      tableName: "calculated_emissions",
      tableColumns: [
        { name: "activity_event_id", dataType: "text"},
        { name: "asset_id", dataType: "text"},
        { name: "geo", dataType: "geometry"},
        { name: "origin_measurement_timestamp", dataType: "timestamptz"},
        { name: "scope", dataType: "integer"},
        { name: "category", dataType: "text"},
        { name: "activity", dataType: "text"},
        { name: "source", dataType: "text"},
        { name: "raw_data", dataType: "decimal(32,16)"},
        { name: "units", dataType: "text"},
        { name: "co2e_amount", dataType: "decimal(32,16)"},
        { name: "co2e_unit", dataType: "text"},
        { name: "n2o_amount", dataType: "decimal(32,16)"},
        { name: "n2o_unit", dataType: "text"},
        { name: "ch4_amount", dataType: "decimal(32,16)"},
        { name: "ch4_unit", dataType: "text"},
        { name: "co2_amount", dataType: "decimal(32,16)"},
        { name: "co2_unit", dataType: "text"},
        { name: "emissions_factor_amount", dataType: "decimal(32,16)"},
        { name: "emissions_factor_unit", dataType: "text"}
      ]
    });

    const emissionsFactorReferenceTable = new dynamodb.Table(this, "CarbonCalculatorEmissionsFactorReferenceTable", {
      partitionKey: { name: "category", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "activity", type: dynamodb.AttributeType.STRING },
      removalPolicy: RemovalPolicy.DESTROY,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST
    });

    // Define DynamoDB Table for calculator output
    this.calculatorOutputTable = new dynamodb.Table(this, "carbonlakeCalculatorOutputTable", {
      partitionKey: { name: "activity_event_id", type: dynamodb.AttributeType.STRING },
      removalPolicy: RemovalPolicy.DESTROY,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST
    });

    this.calculatorFunction = new lambda.Function(this, 'CarbonCalculatorLambdaFunction', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(path.join(__dirname, './lambda')),
      handler: "full_calculator_lambda.lambda_handler",
      timeout: Duration.minutes(5),
      environment: {
        EMISSIONS_FACTOR_TABLE_NAME: emissionsFactorReferenceTable.tableName,
        CALCULATOR_OUTPUT_TABLE_NAME: this.calculatorOutputTable.tableName,
        TRANSFORMED_BUCKET_NAME: this.inputBucket.bucketName,
        REDSHIFT_SECRET: this.outputCluster.secret!.secretArn
      }
    });

    emissionsFactorReferenceTable.grantReadData(this.calculatorFunction);
    this.calculatorOutputTable.grantWriteData(this.calculatorFunction);
    this.inputBucket.grantRead(this.calculatorFunction);
    this.outputCluster.secret!.grantRead(this.calculatorFunction);
    this.calculatorFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ["redshift-data:ExecuteStatement"],
      resources: ['arn:aws:redshift:us-east-1:'+this.account+':cluster:'+this.outputCluster.clusterName],
      effect: iam.Effect.ALLOW
    }))


    checkDuplicatedEmissionFactors();
    //We popupate the Emission Factors DB with data from a JSON file
    //We split into chunks because BatchWriteItem has a limitation of 25 items per batch
    //See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    for (let i = 0; i < emission_factors.length; i += DDB_BATCH_WRITE_ITEM_CHUNK_SIZE) {
      const chunk = emission_factors.slice(i, i + DDB_BATCH_WRITE_ITEM_CHUNK_SIZE);
      new cr.AwsCustomResource(this, `initCarbonLakeEmissionsFactorReferenceTable${i}`, {
        onCreate: {
          service: 'DynamoDB',
          action: 'batchWriteItem',
          parameters: {
            RequestItems: {
              [emissionsFactorReferenceTable.tableName]: this.generateBatch(chunk),
            },
          },
          physicalResourceId: cr.PhysicalResourceId.of(emissionsFactorReferenceTable.tableName + '_initialization')
        },
        policy: cr.AwsCustomResourcePolicy.fromSdkCalls({ resources: [emissionsFactorReferenceTable.tableArn] }),
      });
    }

  }

  private generateBatch = (chunk: IGhgEmissionFactor[]): { PutRequest: { Item: IDdbEmissionFactor } }[] => {
    const result: { PutRequest: { Item: IDdbEmissionFactor; }; }[] = [];
    chunk.forEach((emission_factor) => {
      result.push({ PutRequest: { Item: this.generateItem(emission_factor) } });
    });
    return result;
  };

  private generateItem = (emission_factor: IGhgEmissionFactor): IDdbEmissionFactor => {
    const coefficients = emission_factor.emissions_factor_standards.ghg.coefficients;
    return {
      activity: { S: emission_factor.activity },
      category: { S: emission_factor.category },
      scope: { N: emission_factor.scope },
      emissions_factor_standards: {
        M: {
          ghg: {
            M: {
              coefficients: {
                M: {
                  co2_factor: { S: coefficients.co2_factor },
                  ch4_factor: { S: coefficients.ch4_factor },
                  n2o_factor: { S: coefficients.n2o_factor },
                  biofuel_co2: { S: coefficients.biofuel_co2 },
                  AR4_kgco2e: { S: coefficients['AR4-kgco2e'] },
                  AR5_kgco2e: { S: coefficients['AR5-kgco2e'] },
                  units: { S: coefficients.units },
                }
              },
              last_updated: { S: emission_factor.emissions_factor_standards.ghg.last_updated },
              source: { S: emission_factor.emissions_factor_standards.ghg.source },
              source_origin: { S: emission_factor.emissions_factor_standards.ghg.source_origin },
            }
          }
        }
      }
    };
  }
}

interface IDdbEmissionFactor {
  category: { S: string };
  activity: { S: string };
  scope: { N: string };
  emissions_factor_standards: {
    M: {
      ghg: {
        M: {
          coefficients: {
            M: {
              co2_factor: { S: string }; //TODO use number (I used string because some values are empty in JSON)
              ch4_factor: { S: string }; //TODO use number (I used string because some values are empty in JSON)
              n2o_factor: { S: string }; //TODO use number (I used string because some values are empty in JSON)
              biofuel_co2: { S: string };//TODO use number (I used string because some values are empty in JSON)
              AR4_kgco2e: { S: string }; //TODO use number (I used string because some values are empty in JSON)
              AR5_kgco2e: { S: string }; //TODO use number (I used string because some values are empty in JSON)
              units: { S: string };
            };
          };
          last_updated: { S: string };
          source: { S: string };
          source_origin: { S: string };
        }
      };
    }
  };
}

interface IGhgEmissionFactor {
  category: string;
  activity: string;
  scope: string;
  emissions_factor_standards: {
    ghg: {
      coefficients: {
        co2_factor: string;
        ch4_factor: string;
        n2o_factor: string;
        biofuel_co2: string;
        "AR4-kgco2e": string;
        "AR5-kgco2e": string;
        units: string;
      };
      last_updated: string;
      source: string;
      source_origin: string;
    };
  };
}

function checkDuplicatedEmissionFactors() {
  const categories_and_activities = emission_factors.map(factor => factor.category + "_" + factor.activity);
  const duplicates = categories_and_activities.filter((item, index) => categories_and_activities.indexOf(item) != index);
  if (duplicates.length > 0) {
    throw Error("duplicates found in Emission Factors: " + duplicates);
  }
}
