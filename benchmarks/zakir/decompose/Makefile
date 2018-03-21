CC=clang++
CFLAGS= -march=native -Wall -std=c++11 -Wno-unused-function
INCLUDES=-I.. -I../../../common -I ../../../common/rapidjson/include -I../../../common/aho-corasick/

.PHONY: all debug asm clean

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) decompose.cpp -o bench 

debug:
	$(CC) $(CFLAGS) -O0 -g -DDEBUG $(INCLUDES) decompose.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) decompose.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
