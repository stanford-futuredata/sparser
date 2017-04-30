CLANG=clang
CFLAGS=-O3 -flto
INCLUDES=-I../../common/

.PHONY: all clean

all:
	$(CLANG) $(CFLAGS) $(INCLUDES) ../../common/common.c lazy_ints.c -o run

clean:
	rm -rf run
