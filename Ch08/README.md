
## Prerequisite

- mvn

## Tutorials

### Quickstart locally using Direct Runner

Applies to
- CreateTrainingDataset
- CreateTrainingDataset6

```shell
RUNNER=DirectRunner


mvn compile exec:java \
  -Dexec.mainClass=$MAINCLASS_PATH \
  -Dexec.args="\
    --runner=${RUNNER}"
```

### Run and export to gs using dataflow
Applies to
- CreateTrainingDataset1
- CreateTrainingDataset2
- CreateTrainingDataset3
```shell
# Set envs below is just an example
PROJECT=$(gcloud config get core/project)
STAGING_LOCATION=gs://${PROJECT}/staging
INPUT_LOCATION=gs://${PROJECT}/flights/chapter8/small.csv
OUTPUT_LOCATION=gs://${PROJECT}/flights/chapter8/output/
RUNNER=DataflowRunner

# Run pipeline on Dataflow
mvn compile exec:java \
  -Dexec.mainClass=$MAINCLASS_PATH \
  -Dexec.args="\
    --project=$PROJECT \
    --stagingLocation=$STAGING_LOCATION \
    --input=$INPUT_LOCATION \
    --output=$OUTPUT_LOCATION \
    --runner=$RUNNER"
```

### Create view and log to standard output locally

Applies to
- CreateTrainingDataset4
- CreateTrainingDataset7

```shell
PROJECT=$(gcloud config get core/project)
TRAIN_CSV_LOCATION=gs://${PROJECT}/flights/trainday.csv
RUNNER=DirectRunner
mvn compile exec:java \
  -Dexec.mainClass=$MAINCLASS_PATH \
  -Dexec.args="\
    --traindayCsvPath=$TRAIN_CSV_LOCATION \
    --runner=$RUNNER"
```

## Read from Bigquery and export to gs

Applies to
- CreateTrainingDataset5
- CreateTrainingDataset8

```shell
# Set envs below is just an example
PROJECT=$(gcloud config get core/project)
STAGING_LOCATION=gs://${PROJECT}/staging
OUTPUT_LOCATION=gs://${PROJECT}/flights/chapter8/output/
TRAIN_CSV_LOCATION=gs://${PROJECT}/flights/trainday.csv
TEMP_LOCATION=gs://${PROJECT}/flights/staging
RUNNER=DataflowRunner

# Run pipeline on Dataflow
mvn compile exec:java \
  -Dexec.mainClass=$MAINCLASS_PATH \
  -Dexec.args="\
    --project=$PROJECT \
    --stagingLocation=$STAGING_LOCATION \
    --output=$OUTPUT_LOCATION \
    --traindayCsvPath=$TRAIN_CSV_LOCATION \
    --tempLocation=$TEMP_LOCATION \
    --runner=$RUNNER"
```