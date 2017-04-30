CLANG=clang
CFLAGS=-O3 -flto -march=native

.PHONY: all clean

all:
	${CLANG} ${CFLAGS} vector_buffering.c -o run

clean:
	rm -rf run
