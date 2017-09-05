CC=clang
CFLAGS=-O3 -march=native -Wall -Wno-unused-function
INCLUDES=-I.. -I../../../common

.PHONY: all asm clean

all:
	$(CC) $(CFLAGS) $(INCLUDES) bench.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) bench.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
