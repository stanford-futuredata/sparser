#!/usr/bin/env bash

if [ "$#" -lt 4 ]; then
    echo "At least four arguments required: [--local | --yarn] [query ID] [HDFS file ('hdfs://', default) or local file ('file:///')] [num trials] [--sparser | --spark | --query-only | --read-only ]"
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
  # --conf "spark.dynamicAllocation.enabled=false" \
  # --num-executors 2 \

$SPARK_HOME/bin/spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 \
  --class edu.stanford.sparser.App \
  --master $MASTER target/sparser-1.0.jar \
   10 \
  ${@:2}
