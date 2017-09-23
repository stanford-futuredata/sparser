CC=clang
CFLAGS= -march=native -Wall -Wno-unused-function
INCLUDES=-I. -I/Users/shoumikpalkar/work/sparser/common

.PHONY: all debug asm clean

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) -lz gzip_test.c -o bench

clean:
	rm -rf bench bench.dSYM *.s
