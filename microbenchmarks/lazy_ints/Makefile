CLANG=clang
CFLAGS=-O3 -flto

.PHONY: all clean

all:
	${CLANG} ${CFLAGS} lazy_ints.c -o run

clean:
	rm -rf run
