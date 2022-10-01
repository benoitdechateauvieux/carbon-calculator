import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { aws_lambda as lambda } from 'aws-cdk-lib';
import { aws_s3 as s3 } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as redshift from '@aws-cdk/aws-redshift-alpha';
import { Construct } from 'constructs';
import * as path from 'path';

export interface CarbonCalculatorTestStackProps extends StackProps {
    inputBucket: s3.Bucket;
    outputCluster: redshift.Cluster;
    calculatorFunction: lambda.Function;
}

export class CarbonCalculatorTestStack extends Stack {

    constructor(scope: Construct, id: string, props: CarbonCalculatorTestStackProps) {
        super(scope, id, props);

        // Calculator tests
        const carbonlakeCalculatorTestFunction = new lambda.Function(this, "carbonlakeCalculatorTestLambda", {
            runtime: lambda.Runtime.PYTHON_3_9,
            code: lambda.Code.fromAsset(path.join(__dirname, './test')),
            handler: "test_calculator.lambda_handler",
            timeout: Duration.seconds(60),
            environment: {
                INPUT_BUCKET_NAME: props.inputBucket.bucketName,
                REDSHIFT_SECRET: props.outputCluster.secret!.secretArn,
                CALCULATOR_FUNCTION_NAME: props.calculatorFunction.functionName,
            }
        });
        props.inputBucket.grantReadWrite(carbonlakeCalculatorTestFunction);
        props.calculatorFunction.grantInvoke(carbonlakeCalculatorTestFunction);
        props.outputCluster.secret!.grantRead(carbonlakeCalculatorTestFunction);
        carbonlakeCalculatorTestFunction.addToRolePolicy(new iam.PolicyStatement({
          actions: ["redshift-data:*"],
          resources: ['arn:aws:redshift:us-east-1:'+this.account+':cluster:'+props.outputCluster.clusterName],
          effect: iam.Effect.ALLOW
        }))
        carbonlakeCalculatorTestFunction.role?.addManagedPolicy(
            iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonRedshiftFullAccess")
        )
    }
}
