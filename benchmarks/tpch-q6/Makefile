CLANG=clang
CFLAGS=-O3 -flto -march=native
INCLUDES=-I../../common

.PHONY: all clean

all:
	$(CLANG) $(CFLAGS) $(INCLUDES) q6.c -o run

clean:
	rm -rf run
