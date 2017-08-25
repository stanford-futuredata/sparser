#!/usr/bin/env bash

set -x

$SPARK_HOME/bin/spark-submit --class SparserSpark --master local[1] target/sparser-1.0.jar

