#! /usr/bin/env bash

AVRO_INPUT="/lfs/1/sparser/tweets23g-single-no-limit-uncompressed-projected-text-unique.avro"

if [ "$#" -gt 0 ]; then
  AVRO_INPUT=$1
fi

set -x

./bench $AVRO_INPUT  25 "Donald Trump" "ld T" > q1.txt
./bench $AVRO_INPUT  25 "Obama" "Obam" > q2.txt
./bench $AVRO_INPUT  53 "msa" "msa" > q3.txt
./bench $AVRO_INPUT  25 "realDonaldTrump" "ldTr" > q4.txt

