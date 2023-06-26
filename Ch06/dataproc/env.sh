
#
# These will be value set via gcloud config
#
################################################################################
export REGION=`gcloud config get-value compute/region`
export PROJECT=`gcloud config get-value core/project`
################################################################################

#
# Init script used at beginning of initialization of dataproc cluster
#

# DEPRECATED For datalab initialization provided by google on public gs
#export INSTALL=gs://dataproc-initialization-actions/datalab/datalab.sh

# Some customized user script
#export INSTALL=gs://${PROJECT}/flights/dataproc/install_on_cluster.sh


#
# Dataproc variables
#
export CLUSTER_NAME="ch6cluster"
export SCHEDULED_CLUSTER=1
export DURATION="+2hours"

# Supported machine types
# https://cloud.google.com/dataproc/docs/concepts/compute/supported-machine-types
export MASTER_MACHINE_TYPE=e2-standard-2
export WORKER_MACHINE_TYPE=e2-standard-4
export NUM_OF_WORKERS=2