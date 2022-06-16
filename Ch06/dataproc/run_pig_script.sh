#!/bin/sh

PROJECT_NAME='elite-caster-125113'
CLUSTER_NAME='ch6cluster'
BAYES_VER=${1:-bayes1}

. ./env.sh

gcloud dataproc jobs submit pig \
  --region "$REGION" \
  --cluster $CLUSTER_NAME \
  --params PROJECT_NAME=$PROJECT_NAME \
  --file "${BAYES_VER}".pig