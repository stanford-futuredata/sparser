CC=clang++
CFLAGS=-std=c++11 -march=native -Wall -Wno-unused-function
INCLUDES_DEBUG=-I.. -I../../common -I../../common/rapidjson/include -I../../common/parquet-cpp-firas/src -I../../common/parquet-cpp-firas/build/debug/include
INCLUDES=-I.. -I../../common -I../../common/rapidjson/include -I../../common/parquet-cpp-firas/src -I../../common/parquet-cpp-firas/build/release/include
LDFLAGS_DEBUG=-L../../common/parquet-cpp-firas/build/debug -lparquet -larrow
LDFLAGS=-L../../common/parquet-cpp-firas/build/release -lparquet -larrow

.PHONY: all debug asm clean

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) $(LDFLAGS) query_driver.cpp -o bench


debug:
	$(CC) $(CFLAGS) -O0 -g -DDEBUG $(INCLUDES_DEBUG) $(LDFLAGS_DEBUG) query_driver.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) $(LDFLAGS) query_driver.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
