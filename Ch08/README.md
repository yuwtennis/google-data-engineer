
Quick command references

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
    --runner=DataflowRunner"
```