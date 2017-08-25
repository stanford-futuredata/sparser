CC=clang++
CFLAGS=-std=c++11 -O3 -march=native -Wall -Wno-unused-function
INCLUDES=-I.. -I../../../common -I ../../../common/rapidjson/include

.PHONY: all asm clean

all:
	$(CC) $(CFLAGS) $(INCLUDES) bench.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) bench.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
