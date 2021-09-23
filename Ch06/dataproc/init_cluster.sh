#!/bin/bash

. ./env.sh

if [[ $SCHEDULED_CLUSTER ]]; then
  SCHEDULE=`date --iso-8601=seconds -d "+$DURATION"`
  OPT_SCHEDULE="--expiration-time=$SCHEDULE"
fi

gcloud dataproc clusters create \
  --num-workers=2 \
  --scopes=cloud-platform \
  --worker-machine-type=n1-standard-2 \
  --master-machine-type=n1-standard-4 \
  --region=$REGION \
  --optional-components=JUPYTER \
  --enable-component-gateway \
  --image-version=2.0-debian10 \
  $OPT_SCHEDULE \
  ch6cluster

gcloud dataproc clusters list --region=${REGION}
