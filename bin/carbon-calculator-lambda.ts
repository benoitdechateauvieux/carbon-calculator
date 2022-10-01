#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { CarbonCalculatorLambdaStack } from '../lib/carbon-calculator-lambda-stack';
import { CarbonCalculatorTestStack } from '../lib/carbon-calculator-test-stack';

const app = new cdk.App();
const lambdaStack = new CarbonCalculatorLambdaStack(app, 'CarbonCalculatorLambdaStack');
new CarbonCalculatorTestStack(app, 'CarbonCalculatorTestStack', {
    calculatorFunction: lambdaStack.calculatorFunction,
    inputBucket: lambdaStack.inputBucket,
    outputCluster: lambdaStack.outputCluster
})
