CC=clang++
CFLAGS=-std=c++11 -march=native -Wall -Wno-unused-function
INCLUDES=-I.. -I../../../common -I ../../../common/rapidjson/include

.PHONY: debug asm clean

debug:
	$(CC) $(CFLAGS) -O0 -g -DDEBUG $(INCLUDES) bench.cpp -o bench

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) bench.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) bench.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
