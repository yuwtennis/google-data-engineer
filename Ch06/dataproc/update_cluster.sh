#!/bin/bash

. ./env.sh

NUM_OF_SECONDARY_WORKERS=15
NUM_OF_WORKERS=5

echo "Updating cluster consisting ${NUM_OF_SECONDARY_WORKERS} preemptible workers , ${NUM_OF_WORKERS} workers"

gcloud dataproc clusters update ch6cluster \
  --region ${REGION} \
  --num-workers=${NUM_OF_WORKERS} \
  --num-secondary-workers=${NUM_OF_SECONDARY_WORKERS}


gcloud dataproc clusters list --region $REGION
