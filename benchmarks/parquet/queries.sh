#! /usr/bin/env bash

PARQUET_INPUT="/lfs/1/sparser/tweets-10000-single-no-limit-uncompressed-projected.parquet"

if [ "$#" -gt 0 ]; then
  PARQUET_INPUT=$1
fi

set -x

./bench $PARQUET_INPUT  27 "Donald Trump" "ld T" > q1.txt
./bench $PARQUET_INPUT  27 "Obama" "Obam" > q2.txt
./bench $PARQUET_INPUT  44 "msa" "msa" > q3.txt
./bench $PARQUET_INPUT  27 "realDonaldTrump" "ldTr" > q4.txt

