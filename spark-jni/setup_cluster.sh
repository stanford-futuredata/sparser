#! /usr/bin/env bash

if [ "$#" -lt 1 ]; then
    echo "One argument required: [cluster name]"
    exit 1
fi

CLUSTER_NAME=$1
COMMAND=${2:-setup}

set -x 

gcloud compute instances list --filter=${CLUSTER_NAME} | tr -s ' ' | cut -d ' ' -f \
  5 | grep -v INTERNAL_IP | xargs -I {} fab -i ~/.ssh/google_compute_engine -H {} \
  $COMMAND
