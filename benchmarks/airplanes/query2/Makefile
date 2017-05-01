CC=clang
CFLAGS=-O3 -flto -march=native
INCLUDES=-I.. -I../../../common

.PHONY: all clean

all:
	$(CC) $(CFLAGS) $(INCLUDES) query2.c -o run

clean:
	rm -rf run
