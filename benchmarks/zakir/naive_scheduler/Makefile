CC=clang++
CFLAGS= -march=native -Wall -std=c++11 -Wno-unused-function
INCLUDES=-I.. -I../../../common -I ../../../common/rapidjson/include -I../../../common/aho-corasick/

.PHONY: all debug asm clean

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) query_driver.cpp -o bench 

debug:
	$(CC) $(CFLAGS) -O0 -g -DDEBUG $(INCLUDES) query_driver.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) query_driver.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
