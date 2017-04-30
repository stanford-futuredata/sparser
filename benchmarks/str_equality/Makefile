CLANG=clang
CFLAGS=-O3 -flto

.PHONY: all clean

all:
	${CLANG} ${CFLAGS} str_equality.c -o run

clean:
	rm -rf run
