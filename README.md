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
- edit:
config/pm-analytics.properties to give deployment parameters (CDAP host, port and namespace)

- Deploy and run CDAP app, flowlets and service:
`bin/startup.sh config/pm-analytics.properties`

- load raw data:
```
batch load:
cdap cli -u http://localhost:11015/test load stream c26Stream /Users/mz/test/volvo/data/Volvo-Cleaned-data-ordered.txt

single load:
curl -w"\n" -X POST "localhost:11015/v3/namespaces/test/streams/c26Stream" -d "20558344,Cupertino,US,37.32472229,-122.0046082,4490845,95014,California,Junipero Serra Freeway ,1/12/2015 18:14,1214,Davis,US,38.55670929,-121.7704773,4312293,95616,California,Santa Rosa Street 1555,1/12/2015 15:33,20659034,0,0,0,0,0,0,37.32472229,-122.0046082,38.55670929,-121.7704773,0,20659034,business,1/12/2015 18:14,1/12/2015 15:33,15015"
```

- Run Workflow/Spark to train models
```
CDAP cli:
cdap cli start mapreduce pm-analytics.c26ModelTrainWorkflow
or REST API:
curl -w"\n" -X POST "localhost:11015/v3/namespaces/test/apps/pm-analytics/workflows/c26ModelTrainWorkflow/start"
```

- data query
```
Total trip count:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trip/count

All vins of the trips:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trip/vins

All trips of a vin:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trip/{vin}

Total feature count:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/count

All vins of the features:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/vins

All features of a vin:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/{vin}

Total model count:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/count

All vins of the model:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/vins

All models of a vin:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/{vin}

Query models of a vin:
http://localhost:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/{vin}?origin=xxx&timeOfDay=yyy&dayOfWeek=Monday&dayType=weekend
```

## Swagger API demo




