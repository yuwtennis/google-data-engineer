#!/usr/bin/env bash

REGION=$(gcloud config get compute/region)
PROJECT_ID=$(gcloud config get core/project)
BUCKET_NAME=${PROJECT_ID}-jp
TRAIN_DATA=/gcs/${BUCKET_NAME}/flights/train.csv
EVAL_DATA=/gcs/${BUCKET_NAME}/flights/test.csv
OUTPUT_DIR=/gcs/${BUCKET_NAME}/flights/output/
CONTAINER_IMAGE_URI=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOS_NAME:-custom-training}/flights:${TAG:-$(git rev-parse HEAD)}
MACHINE_TYPE=n2-standard-4

gcloud ai custom-jobs create \
  --region=asia-northeast1 \
  --display-name=flights \
  --config=config.yaml \
  --args=--traindata="$TRAIN_DATA",--evaldata="$EVAL_DATA",--output="$OUTPUT_DIR" \
  --worker-pool-spec=replica-count=1,machine-type=${MACHINE_TYPE},container-image-uri="$CONTAINER_IMAGE_URI"

