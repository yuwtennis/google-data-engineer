#!/bin/sh

PROJECT_NAME='elite-caster-125113'
CLUSTER_NAME='ch6cluster'

. ./env.sh

gcloud dataproc jobs submit pig \
  --region $REGION \
  --cluster $CLUSTER_NAME \
  --params PROJECT_NAME=$PROJECT_NAME \
  --file bayes.pig