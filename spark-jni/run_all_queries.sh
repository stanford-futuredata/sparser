#! /usr/bin/env bash

set -e
set -x

mkdir -p timings

# Twitter Queries
./run.sh --yarn twitter1 /user/fabuzaid21/tweets68g.json 6 --read-only > timings/twitter-read-only-hdfs.txt

./run.sh --yarn twitter1 /user/fabuzaid21/tweets68g.json 6 --spark > timings/twitter1-spark-hdfs.txt
./run.sh --yarn twitter1 /user/fabuzaid21/tweets68g.parquet 6 --spark > timings/twitter1-spark-hdfs-parquet.txt
./run.sh --yarn twitter1 /user/fabuzaid21/tweets68g.json 6 --sparser > timings/twitter1-sparser-hdfs.txt
./run.sh --yarn twitter1 /user/fabuzaid21/tweets68g.json 6 --query-only > timings/twitter1-query-only-hdfs.txt

./run.sh --yarn twitter2 /user/fabuzaid21/tweets68g.json 6 --spark > timings/twitter2-spark-hdfs.txt
./run.sh --yarn twitter2 /user/fabuzaid21/tweets68g.parquet 6 --spark > timings/twitter2-spark-hdfs-parquet.txt
./run.sh --yarn twitter2 /user/fabuzaid21/tweets68g.json 6 --sparser > timings/twitter2-sparser-hdfs.txt
./run.sh --yarn twitter2 /user/fabuzaid21/tweets68g.json 6 --query-only > timings/twitter2-query-only-hdfs.txt

./run.sh --yarn twitter3 /user/fabuzaid21/tweets68g.json 6 --spark > timings/twitter3-spark-hdfs.txt
./run.sh --yarn twitter3 /user/fabuzaid21/tweets68g.parquet 6 --spark > timings/twitter3-spark-hdfs-parquet.txt
./run.sh --yarn twitter3 /user/fabuzaid21/tweets68g.json 6 --sparser > timings/twitter3-sparser-hdfs.txt
./run.sh --yarn twitter3 /user/fabuzaid21/tweets68g.json 6 --query-only > timings/twitter3-query-only-hdfs.txt

./run.sh --yarn twitter4 /user/fabuzaid21/tweets68g.json 6 --spark > timings/twitter4-spark-hdfs.txt
./run.sh --yarn twitter4 /user/fabuzaid21/tweets68g.parquet 6 --spark > timings/twitter4-spark-hdfs-parquet.txt
./run.sh --yarn twitter4 /user/fabuzaid21/tweets68g.json 6 --sparser > timings/twitter4-sparser-hdfs.txt
./run.sh --yarn twitter4 /user/fabuzaid21/tweets68g.json 6 --query-only > timings/twitter4-query-only-hdfs.txt

# Zakir Queries
./run.sh --yarn zakir1 /user/fabuzaid21/zakir652g.json 6 --read-only > timings/zakir-read-only-hdfs.txt

./run.sh --yarn zakir1 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir1-spark-hdfs.txt
./run.sh --yarn zakir1 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir1-sparser-hdfs.txt
./run.sh --yarn zakir1 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir1-query-only-hdfs.txt

./run.sh --yarn zakir2 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir2-spark-hdfs.txt
./run.sh --yarn zakir2 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir2-sparser-hdfs.txt
./run.sh --yarn zakir2 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir2-query-only-hdfs.txt

./run.sh --yarn zakir3 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir3-spark-hdfs.txt
./run.sh --yarn zakir3 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir3-sparser-hdfs.txt
./run.sh --yarn zakir3 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir3-query-only-hdfs.txt

./run.sh --yarn zakir4 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir4-spark-hdfs.txt
./run.sh --yarn zakir4 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir4-sparser-hdfs.txt
./run.sh --yarn zakir4 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir4-query-only-hdfs.txt

./run.sh --yarn zakir5 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir5-spark-hdfs.txt
./run.sh --yarn zakir5 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir5-sparser-hdfs.txt
./run.sh --yarn zakir5 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir5-query-only-hdfs.txt

./run.sh --yarn zakir6 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir6-spark-hdfs.txt
./run.sh --yarn zakir6 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir6-sparser-hdfs.txt
./run.sh --yarn zakir6 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir6-query-only-hdfs.txt

./run.sh --yarn zakir7 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir7-spark-hdfs.txt
./run.sh --yarn zakir7 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir7-sparser-hdfs.txt
./run.sh --yarn zakir7 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir7-query-only-hdfs.txt

./run.sh --yarn zakir8 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir8-spark-hdfs.txt
./run.sh --yarn zakir8 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir8-sparser-hdfs.txt
./run.sh --yarn zakir8 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir8-query-only-hdfs.txt

./run.sh --yarn zakir9 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir9-spark-hdfs.txt
./run.sh --yarn zakir9 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir9-sparser-hdfs.txt
./run.sh --yarn zakir9 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir9-query-only-hdfs.txt

./run.sh --yarn zakir10 /user/fabuzaid21/zakir652g.json 6 --spark > timings/zakir10-spark-hdfs.txt
./run.sh --yarn zakir10 /user/fabuzaid21/zakir652g.json 6 --sparser > timings/zakir10-sparser-hdfs.txt
./run.sh --yarn zakir10 /user/fabuzaid21/zakir652g.json 6 --query-only > timings/zakir10-query-only-hdfs.txt

