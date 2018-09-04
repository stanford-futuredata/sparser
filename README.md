# sparser

This code base implements Sparser, raw filtering for faster analytics over raw data. Sparser can parse JSON, Avro, and Parquet data up to 22x faster than the state of the art. For more details, check out [our paper published at VLDB 2018](http://www.vldb.org/pvldb/vol11/p1576-palkar.pdf).

See the `demo-repl` directory for a brief example. To run it:

    # update rapidjson submodule
    git submodule init
    git submodule update
    cd demo-repl
    make
    ./bench /path/to/large/file.json

Then enter `1` at the `Sparser>` prompt.

Sparser itself is just a header file and only depends on standard C libraries available
on most systems.
