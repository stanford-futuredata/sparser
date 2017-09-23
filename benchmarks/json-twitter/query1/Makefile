CC=clang++
CFLAGS= -march=native -Wall -Wno-unused-function
INCLUDES=-I.. -I../../../common -I ../../../common/rapidjson/include -I../../../common/aho-corasick/

.PHONY: all debug asm clean

all:
	$(CC) $(CFLAGS) -O3 $(INCLUDES) bench.cpp ../../../common/aho-corasick/acism.c\
	 ../../../common/aho-corasick/acism_create.c  ../../../common/aho-corasick/acism_dump.c  ../../../common/aho-corasick/acism_file.c	-o bench 


debug:
	$(CC) $(CFLAGS) -O0 -g -DDEBUG $(INCLUDES) bench.cpp -o bench

asm:
	$(CC) $(CFLAGS) $(INCLUDES) bench.cpp -S

clean:
	rm -rf bench bench.dSYM *.s
