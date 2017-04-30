CLANG=clang
CFLAGS=-O3 -flto -march=native
INCLUDES=-I../../common/

.PHONY: all clean

all:
	$(CLANG) $(CFLAGS) $(INCLUDES) ../../common/common.c vector_delimiters.c -o run

clean:
	rm -rf run
