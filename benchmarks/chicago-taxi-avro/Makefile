CC=clang++
CFLAGS=-std=c++11 -march=native -Wall -Wno-unused-function
INCLUDES=-I.. -I../../common -I../../common/rapidjson/include -I../../common/avro/include
LDFLAGS=-L../../common/avro/lib -lavro 

.PHONY: all debug asm clean

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) query_driver.cpp $(LDFLAGS) -o bench


debug:
	$(CC) $(CFLAGS) -O0 -g -DDEBUG $(INCLUDES) query_driver.cpp $(LDFLAGS) -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) $(LDFLAGS) query_driver.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
