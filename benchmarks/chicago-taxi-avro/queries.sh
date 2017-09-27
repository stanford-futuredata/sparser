#! /usr/bin/env bash

if [ "$#" -lt 0 ]; then
    echo "At least one argument required: [path/to/avro_file]"
    exit 1
fi

AVRO_INPUT=$1

set -x

./bench $AVRO_INPUT  1 "d59a55fcbd9ef11987cadd1a2dc93149edace06e23c37ad2544aa2e7f7a164590c4c280adb351bad4a6e1d700c503d005899078b942227bd1c7a61da013bbb04" "d59a" > q1.txt
./bench $AVRO_INPUT  16 "Top Cab Affiliation" "b Af" > q2.txt
./bench $AVRO_INPUT  16 "C & D Cab Co Inc" "& D " > q3.txt
./bench $AVRO_INPUT  16 "3253 - 91138 Gaither Cab Co." "38 G" > q4.txt

