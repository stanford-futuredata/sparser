CC=clang++
CFLAGS=-O3 -flto -march=native
INCLUDES=-I../../../common -I../rapidjson/include

.PHONY: all clean

all:
	$(CC) $(CFLAGS) $(INCLUDES) nesting_test.cpp -o run

clean:
	rm -rf run
