#! /usr/bin/env bash

PWD=`pwd`

set -x

git clone https://github.com/apache/parquet-cpp.git parquet-cpp-debug
cd parquet-cpp-debug/src/parquet && ln -nsf ../../../column_reader.h
cd ../.. && cmake -DCMAKE_BUILD_TYPE=Debug -DPARQUET_BUILD_TESTS=Off .
make -j

cd $PWD

git clone https://github.com/apache/parquet-cpp.git parquet-cpp-release
cd parquet-cpp-release/src/parquet && ln -nsf ../../../column_reader.h
cd ../.. && cmake -DCMAKE_BUILD_TYPE=Release -DPARQUET_BUILD_TESTS=Off .
make -j
