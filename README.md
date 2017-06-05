# pm-analytics cdap-app

## Overview
Software components
- CDAP app: c26Analytics
- Flowlets: c26TripFlowlet, c26FeatureFlowlet
- Spark job: SparkAssociationRule
- Workflow: c26ModelTrainWorkflow

Datasets
- c26TripStore: store trip raw data
- c26FeatureStore: store the features extracted from trip raw data
- c26TrainStore: keep the recoreds of recent updated features that will be put into model training process
- c26ModelStore: store the models from model training spark job

## Building
mvn clean install

## Running
- edit config/pm-analytics.properties to give deployment parameters (CDAP host, port and namespace)
- bin/startup.sh
- load raw data
- data query

## Swagger API demo