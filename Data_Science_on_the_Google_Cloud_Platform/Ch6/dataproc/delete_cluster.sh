#!/bin/bash

. ./env.sh

gcloud dataproc clusters list --filter=clusterName=${CLUSTER_NAME} --region ${REGION} | grep 'RUNNING' && ret=$?

if [[ $ret == 0 ]]; then
  gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION} --quiet
else
  echo "There is not running cluster name ${CLUSTER_NAME} in region ${REGION}"
fi
