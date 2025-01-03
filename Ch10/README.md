I have reimplemented Ch08 with help of DDD to promote as production code [Ch08](../Ch08)

Main class will be named as [RealTimePipeline.java](../Ch08/src/main/java/org/example/RealTimePipeline.java)

As a high level view, below functions will be implemented.

* Ingest events from Google PubSub
* Call oneline prediction API built in Chapter 09

```shell
PROJECT=$(gcloud config get core/project)
OUTPUT_LOCATION=gs://${PROJECT}/flights/chapter10/output/
DEPARTURE_DELAY_CSV_PATH=gs://${PROJECT}/flights/chapter8/output/delays.csv
AI_PLATFORM_PROJ_ID=$PROJECT
AI_PLATFORM_LOCATION=$(gcloud config get compute/region)
AI_PLATFORM_ENDPOINT_ID="3976559723712348160"
TEMP_LOCATION=gs://${PROJECT}/flights/staging
RUNNER=DirectRunner
mvn compile exec:java \
  -D exec.mainClass=org.example.RealTimePipeline \
  -Dexec.args="\
    --runner=$RUNNER \
    --project=$PROJECT \
    --tempLocation=$TEMP_LOCATION \
    --output=$OUTPUT_LOCATION \
    --departureDelayCsvPath=$DEPARTURE_DELAY_CSV_PATH \
    --aiPlatformProjectId=$AI_PLATFORM_PROJ_ID \
    --aiPlatformLocation=$AI_PLATFORM_LOCATION \
    --aiPlatformEndpointId=$AI_PLATFORM_ENDPOINT_ID"
```

```shell
PROJECT=$(gcloud config get core/project)
OUTPUT_LOCATION=gs://${PROJECT}/flights/chapter10/output/
STAGING_LOCATION=gs://${PROJECT}/staging
DEPARTURE_DELAY_CSV_PATH=gs://${PROJECT}/flights/chapter8/output/delays.csv
AI_PLATFORM_PROJ_ID=$PROJECT
AI_PLATFORM_LOCATION=$(gcloud config get compute/region)
AI_PLATFORM_ENDPOINT_ID="3976559723712348160"
TEMP_LOCATION=gs://${PROJECT}/flights/staging
RUNNER=DataflowRunner
mvn compile exec:java \
  -D exec.mainClass=org.example.RealTimePipeline \
  -Dexec.args="\
    --runner=$RUNNER \
    --project=$PROJECT \
    --stagingLocation=$STAGING_LOCATION \
    --tempLocation=$TEMP_LOCATION \
    --output=$OUTPUT_LOCATION \
    --departureDelayCsvPath=$DEPARTURE_DELAY_CSV_PATH \
    --aiPlatformProjectId=$AI_PLATFORM_PROJ_ID \
    --aiPlatformLocation=$AI_PLATFORM_LOCATION \
    --aiPlatformEndpointId=$AI_PLATFORM_ENDPOINT_ID"
```