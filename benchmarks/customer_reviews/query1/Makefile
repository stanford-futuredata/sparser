CC=clang++
CFLAGS=-O3 -flto -march=native
INCLUDES=-I../../../common -I../rapidjson/include

.PHONY: all clean

all:
	$(CC) $(CFLAGS) $(INCLUDES) query1.cpp -o run

clean:
	rm -rf run
