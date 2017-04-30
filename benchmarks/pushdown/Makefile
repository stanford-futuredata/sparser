CLANG=clang
CFLAGS=-O3
INCLUDES=-I../../common/

.PHONY: all clean

all:
	$(CLANG) $(CFLAGS) $(INCLUDES) count_data.c -o run

clean:
	rm -rf run
