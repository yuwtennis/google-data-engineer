#!/bin/bash

set -x

WORKDIR=/work
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

if [[ "${ROLE}" == 'Master' ]]; then
  mkdir $WORKDIR
  cd $WORKDIR
  git clone \
    https://github.com/GoogleCloudPlatform/data-science-on-gcp
fi
