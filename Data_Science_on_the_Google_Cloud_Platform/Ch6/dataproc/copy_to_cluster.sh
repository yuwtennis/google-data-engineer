#!/bin/bash

BUCKET=cloud-training-demos-ml
ZONE=us-central1-a

INSTALL=gs://$BUCKET/flights/dataproc/install_on_cluster.sh

# upload install file
gsutil cp install_on_cluster.sh $INSTALL
