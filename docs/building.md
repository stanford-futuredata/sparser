To build the avro C library:

```
brew install jansson
cd $SPARSER_HOME/common/avro-c-1.8.2
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$HOME/work/sparser/common/avro -DCMAKE_OSX_ARCHITECTURES=x86_64 -DJANSSON_INCLUDE_DIR=/usr/local/Cellar/jansson/2.10/include/
make
make test
make install
```
