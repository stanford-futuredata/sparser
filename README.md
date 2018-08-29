# sparser

This code base implements Sparser, raw filtering for faster analytics over raw
data. Sparser can parse JSON, Avro, and Parquet data up to 22x faster than the
state of the art. For more details, check out [our paper published at VLDB
2018](http://www.vldb.org/pvldb/vol11/p1576-palkar.pdf).

To run and build a demo of Sparser:

1. Clone the repo recursively: `git clone --recursive`
2. Build `json/rapidjson` (See the README there for build instructions.)
3. Run `make` in the `demo-repl` directory to run the command-line demo.
4. Run `make` in the `spark` directory to run the Spark demo. (See the README
   there for instructions on installing dependencies.)

Sparser itself is just a header file and only depends on standard C libraries available
on most systems.
