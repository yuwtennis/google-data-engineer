#!/bin/bash

. ./env.sh
gcloud compute ssh --zone=`gcloud config get-value compute/zone` --ssh-flag='-D 1080' --ssh-flag='-N' --ssh-flag='-n' ${CLUSTER_NAME}-m
