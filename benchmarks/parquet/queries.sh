#! /usr/bin/env bash

SNAPPY_INPUT="/lfs/1/sparser/tweets23g-single-no-limit-snappy-projected-text-unique.parquet"
UNCOMPRESSED_INPUT="/lfs/1/sparser/tweets23g-single-no-limit-uncompressed-projected-text-unique.parquet"

if [ "$#" -eq 0 ]; then
  echo "Usage: ./queries.sh [--snappy|--uncompressed]"
  exit 1
fi

if [ $1 == "--snappy" ]; then
  INPUT=$SNAPPY_INPUT
  OUTPUT_DIR=snappy
elif [ $1 == "--uncompressed" ]; then
  INPUT=$UNCOMPRESSED_INPUT
  OUTPUT_DIR=uncompressed
else
  echo "Usage: ./queries.sh [--snappy|--uncompressed]"
  exit 1
fi

mkdir -p $OUTPUT_DIR

set -x

./bench $INPUT 27 "Donald Trump" "ld T" > ${OUTPUT_DIR}/q1.txt
./bench $INPUT 27 "Obama" "Obam" > ${OUTPUT_DIR}/q2.txt
./bench $INPUT 51 "msa" "msa" > ${OUTPUT_DIR}/q3.txt
./bench $INPUT 27 "realDonaldTrump" "ldTr" > ${OUTPUT_DIR}/q4.txt

