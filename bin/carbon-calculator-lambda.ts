#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { CarbonCalculatorLambdaStack } from '../lib/carbon-calculator-lambda-stack';

const app = new cdk.App();
new CarbonCalculatorLambdaStack(app, 'CarbonCalculatorLambdaStack');
