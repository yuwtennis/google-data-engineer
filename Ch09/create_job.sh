#!/usr/bin/env bash

TRAIN_DATA=/gcs/elite-caster-125113-jp/flights/train.csv
EVAL_DATA=/gcs/elite-caster-125113-jp/flights/test.csv
OUTPUT_DIR=/gcs/elite-caster-125113-jp/flights/output/

gcloud ai custom-jobs create \
  --region=asia-northeast1 \
  --display-name=flights \
  --config=config.yaml \
  --args=--traindata="$TRAIN_DATA",--evaldata="$EVAL_DATA",--output="$OUTPUT_DIR" \
  --worker-pool-spec=container-image-uri="$CONTAINER_IMAGE_URI"

