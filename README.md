# sparser

This code base implements Sparser, raw filtering for faster analytics over raw data. Sparser can parse JSON, Avro, and Parquet data up to 22x faster than the state of the art. For more details, check out [our paper published at VLDB 2018](http://www.vldb.org/pvldb/vol11/p1576-palkar.pdf).

See the `demo-repl` directory for a brief example. To run it:

1. Build `json/rapidjson` (see the instructions there on how to do that)
2. `make` in the `demo-repl` directory.

Sparser itself is just a header file and only depends on standard C libraries available
on most systems.
