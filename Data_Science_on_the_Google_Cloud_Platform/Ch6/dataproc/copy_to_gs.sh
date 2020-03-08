#!/bin/bash

. ./env.sh

# upload install file
gsutil cp install_on_cluster.sh $INSTALL

# list uploaded file
gsutil ls $INSTALL
