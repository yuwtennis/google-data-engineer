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