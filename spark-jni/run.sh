#!/usr/bin/env bash

if [ "$#" -lt 3 ]; then
    echo "At least three arguments required: [--local | --yarn] [HDFS file] [num trials] [--sparser (optional)]"
    exit 1
fi

MASTER=$1

if [ $MASTER == "--local" ]; then
    MASTER=local[1]
elif [ $MASTER == "--yarn" ]; then
    MASTER=yarn
else
    echo "First argument must be either --local or --yarn"
    exit 1
fi

set -x

$SPARK_HOME/bin/spark-submit --class edu.stanford.sparser.App \
  --master $MASTER target/sparser-1.0.jar \
  2 "Putin,Russia" "created_at,user.name" \
  ${@:2}
