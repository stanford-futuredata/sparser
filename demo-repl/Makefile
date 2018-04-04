CC=clang
CFLAGS=-std=c++11 -march=native -Wall -Wno-unused-function -lstdc++
INCLUDES=-I../sparser -I../json -I../json/rapidjson/include

.PHONY: all debug asm clean

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) query_driver.cpp -o bench


debug:
	$(CC) $(CFLAGS) -O0 -g -DDEBUG $(INCLUDES) bench.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) bench.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
