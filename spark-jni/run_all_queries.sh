! /usr/bin/env bash

set -x

mkdir -p timings

./run.sh --yarn twitter1 /user/fabuzaid21/tweets23g.json 6 --sparser > timings/twitter1-sparser-hdfs.txt
./run.sh --yarn twitter1 /user/fabuzaid21/tweets23g.json 6 --spark > timings/twitter1-spark-hdfs.txt

./run.sh --yarn twitter2 /user/fabuzaid21/tweets23g.json 6 --sparser > timings/twitter2-sparser-hdfs.txt
./run.sh --yarn twitter2 /user/fabuzaid21/tweets23g.json 6 --spark > timings/twitter2-spark-hdfs.txt

./run.sh --yarn twitter3 /user/fabuzaid21/tweets23g.json 6 --sparser > timings/twitter3-sparser-hdfs.txt
./run.sh --yarn twitter3 /user/fabuzaid21/tweets23g.json 6 --spark > timings/twitter3-spark-hdfs.txt

./run.sh --yarn twitter4 /user/fabuzaid21/tweets23g.json 6 --sparser > timings/twitter4-sparser-hdfs.txt
./run.sh --yarn twitter4 /user/fabuzaid21/tweets23g.json 6 --spark > timings/twitter4-spark-hdfs.txt

./run.sh --yarn zakir1 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir1-sparser-hdfs.txt
./run.sh --yarn zakir1 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir1-spark-hdfs.txt

./run.sh --yarn zakir2 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir2-sparser-hdfs.txt
./run.sh --yarn zakir2 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir2-spark-hdfs.txt

./run.sh --yarn zakir3 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir3-sparser-hdfs.txt
./run.sh --yarn zakir3 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir3-spark-hdfs.txt

./run.sh --yarn zakir4 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir4-sparser-hdfs.txt
./run.sh --yarn zakir4 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir4-spark-hdfs.txt

./run.sh --yarn zakir5 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir5-spark-hdfs.txt
./run.sh --yarn zakir5 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir5-sparser-hdfs.txt

./run.sh --yarn zakir6 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir6-sparser-hdfs.txt
./run.sh --yarn zakir6 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir6-spark-hdfs.txt

./run.sh --yarn zakir7 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir7-sparser-hdfs.txt
./run.sh --yarn zakir7 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir7-spark-hdfs.txt

./run.sh --yarn zakir8 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir8-sparser-hdfs.txt
./run.sh --yarn zakir8 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir8-spark-hdfs.txt

./run.sh --yarn zakir9 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir9-sparser-hdfs.txt
./run.sh --yarn zakir9 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir9-spark-hdfs.txt

./run.sh --yarn zakir10 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir10-sparser-hdfs.txt
./run.sh --yarn zakir10 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir10-spark-hdfs.txt
