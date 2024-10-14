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

## How to run a job

```shell
export PROJECT_ID=$(gcloud config get core/project)
export REGION=$(gcloud config get compute/region)
export REPOS_NAME=custom-training
export IMAGE_TAG=YOUR_IMAGE_TAG
export CONTAINER_IMAGE_URI=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOS_NAME}/${IMAGE_TAG}

./create_job.sh
```