#!/bin/bash

gcloud dataproc clusters create \
  --num-workers=2 \
  --scopes=cloud-platform \
  --worker-machine-type=n1-standard-2 \
  --master-machine-type=n1-standard-4 \
  --zone=$ZONE \
  --initialization-actions=$INSTALL \
  ch6cluster
