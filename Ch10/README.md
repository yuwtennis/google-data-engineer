I have reimplemented Ch08 with help of DDD to promote as production code [Ch08](../Ch08)

Main class will be named as [RealTimePipeline.java](../Ch08/src/main/java/org/example/RealTimePipeline.java)

As a high level view, below functions will be implemented.

* Ingest events from Google PubSub
* Call oneline prediction API built in Chapter 09

## Instructions

### Setting up Bigtable

Create a instance

```shell
gcloud bigtable instances create flights \
  --display-name="flights" \
  --cluster-config=id=datascienceongcp,nodes=1,zone=asia-northeast1-a
```

Create a table

```shell
gcloud bigtable instances tables create predictions --instance=flights --column-families=FL
```

Reading data

See full example of cbt
https://cloud.google.com/bigtable/docs/create-instance-write-data-cbt-cli#local-shell

```shell
gcloud components install cbt
cbt -project elite-caster-125113 -instance flights read predictions
```

Clean up
```shell
gcloud bigtable instances delete flights
```

### Running apache beam

#### Using direct runner

```shell
PROJECT=$(gcloud config get core/project)
OUTPUT_LOCATION=gs://${PROJECT}/flights/chapter10/output/
GOOGLE_PROJECT_ID=$PROJECT
DEPARTURE_DELAY_CSV_PATH=gs://${PROJECT}/flights/chapter8/output/delays.csv
AI_PLATFORM_LOCATION=$(gcloud config get compute/region)
AI_PLATFORM_ENDPOINT_ID=MY_ENDOINT_ID
TEMP_LOCATION=gs://${PROJECT}/flights/staging
BIGTABLE_INSTANCE_ID=flights
BIGTABLE_TABLE_ID=predictions
RUNNER=DirectRunner
mvn compile exec:java \
  -D exec.mainClass=org.example.App \
  -Dexec.args="\
    --runner=$RUNNER \
    --tempLocation=$TEMP_LOCATION \
    --output=$OUTPUT_LOCATION \
    --googleProjectId=$GOOGLE_PROJECT_ID \
    --departureDelayCsvPath=$DEPARTURE_DELAY_CSV_PATH \
    --aiPlatformLocation=$AI_PLATFORM_LOCATION \
    --aiPlatformEndpointId=$AI_PLATFORM_ENDPOINT_ID \
    --bigtableInstanceId=$BIGTABLE_INSTANCE_ID \
    --bigtableTableId=$BIGTABLE_TABLE_ID"
```

#### Using dataflow , tbc

```shell
PROJECT=$(gcloud config get core/project)
OUTPUT_LOCATION=gs://${PROJECT}/flights/chapter10/output/
STAGING_LOCATION=gs://${PROJECT}/staging
GOOGLE_PROJECT_ID=$PROJECT
DEPARTURE_DELAY_CSV_PATH=gs://${PROJECT}/flights/chapter8/output/delays.csv
AI_PLATFORM_LOCATION=$(gcloud config get compute/region)
AI_PLATFORM_ENDPOINT_ID=MY_ENDOINT_ID
TEMP_LOCATION=gs://${PROJECT}/flights/staging
RUNNER=DataflowRunner
mvn compile exec:java \
  -D exec.mainClass=org.example.App \
  -Dexec.args="\
    --runner=$RUNNER \
    --stagingLocation=$STAGING_LOCATION \
    --tempLocation=$TEMP_LOCATION \
    --googleProjectId=$GOOGLE_PROJECT_ID \
    --output=$OUTPUT_LOCATION \
    --departureDelayCsvPath=$DEPARTURE_DELAY_CSV_PATH \
    --aiPlatformLocation=$AI_PLATFORM_LOCATION \
    --aiPlatformEndpointId=$AI_PLATFORM_ENDPOINT_ID"
```
