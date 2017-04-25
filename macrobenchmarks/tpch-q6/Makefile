CLANG=clang
CFLAGS=-O3 -flto -march=native

.PHONY: all clean

all:
	${CLANG} ${CFLAGS} q6.c -o run

clean:
	rm -rf run
