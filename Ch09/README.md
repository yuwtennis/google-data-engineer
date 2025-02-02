This chapter uses Vertex AI to run custom training jobs.

## Pre-requisite

- Images for running custom training jobs are pushed to Artifact Registry
- Training and Test datasets are saved on Google Cloud Storage

## Architecture

```markdown
|-----------|                 |-------------------|
|           | <--- 1.Pull --- | Artifact Registry |
|           |                 |-------------------|
|           |
| Vertex AI |                 |-----|
|           | <--- 2.Pull --- |     |
|           |                 | GCS |
|           | --- 3.Save ---> |     |
|-----------|                 |-----|
```

1. Pull Image from `Artifact Registry`
2. Pull dataset from `GCS`
3. Save model and checkpoints to `GCS`

## Tutorial

## Deploy endpoint

Register a model
```shell
make import-model GS_ARTIFACT_URI=MY_GS_URI
```

Create endpoint

```shell
gcloud ai endpoints create \
  --display-name flights \
  --region asia-northeast1
```

Deploy the model

```shell
make deploy ENDPOINT=MY_ENDPOINT MODEL_ID=MY_MODEL_ID
```
Undeploy the model
```shell
gcloud ai endpoints describe ${ENDPOINT_ID} \
  --region asia-northeast1
gcloud ai endpoints undeploy-model ${ENDPOINT_ID} \
  --region asia-northeast1 \
  --deployed-model-id ${DEPLOYED_MODEL_ID}
```

Terminate the endpoint when you are done

```shell
gcloud ai endpoints delete ${ENDPOINT_ID} \
    	--region=asia-northeast1
```

## Run a training job

```shell
poetry run make train
```

## Get online prediction result

1. Set path to service account JSON private key file
```shell
export GOOGLE_APPLICATION_CREDENTIALS=PATH_TO_YOUR_FILE
```

2. Run script

```shell
poetry run python3 scripts/get_online_prediction.py --location REGION_NAME --endpoint_id ENDPOINT_ID
```