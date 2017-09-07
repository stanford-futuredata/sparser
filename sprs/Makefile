CC=clang
CC=clang
CFLAGS=-march=native -Wall -Wno-unused-function
INCLUDES=-I../common/
TARGET=sprs

.PHONY: all debug clean

all:
	$(CC) $(INCLUDES) $(CFLAGS) -O3 -o $(TARGET) sprs.c

debug:
	$(CC) $(INCLUDES) $(CFLAGS) -O0 -g -DDEBUG -o $(TARGET) sprs.c

clean:
	rm -rf $(TARGET)
