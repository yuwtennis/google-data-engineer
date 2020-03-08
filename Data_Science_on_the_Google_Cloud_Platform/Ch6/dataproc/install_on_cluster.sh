#!/bin/bash

USER=ywatanabe
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

if [[ "${ROLE}" == 'Master' ]]; then
  cd home/$USER
  git clone \
    https://github.com/GoogleCloudPlatform/data-science-on-gcp
fi
