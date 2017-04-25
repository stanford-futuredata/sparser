CC=clang
CFLAGS=-O3 -flto -march=native
INCLUDES=-I../common/

.PHONY: all clean

all:
	$(CC) $(CFLAGS) $(INCLUDES) ../common/parser.c query1.c -o run

clean:
	rm -rf run
