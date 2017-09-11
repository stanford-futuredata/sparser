CC=clang++
CFLAGS=-std=c++11 -O3 -flto -march=native -g
INCLUDES=-I.. -I../../../common -I ../../../common/rapidjson/include

.PHONY: all clean

all:
	$(CC) -DDEBUG $(CFLAGS) $(INCLUDES) bench.cpp -o bench

clean:
	rm -rf bench bench.dSYM
