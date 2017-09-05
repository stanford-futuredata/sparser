UNAME := $(shell uname -s)

CC=clang++
CFLAGS=-c -fPIC -march=native
LDFLAGS=-shared -Wl
INCLUDES=-I$(JAVA_HOME)/include \
	 -I../common/rapidjson/include \
	 -I../common

ifeq ($(UNAME),Linux)
  INCLUDES += -I$(JAVA_HOME)/include/linux
  TARGET=libsparser.so
else ifeq ($(UNAME),Darwin)
  JAVA_HOME=$(shell /usr/libexec/java_home)
  INCLUDES += -I$(JAVA_HOME)/include/darwin
  TARGET=libsparser.dylib
endif

.PHONY: all clean

all:	$(TARGET)

$(TARGET): SparserSpark.o
	$(CC) $(INCLUDES) -o $(TARGET) $(LDFLAGS) SparserSpark.o

SparserSpark.o: SparserSpark.cpp
	$(CC) $(CFLAGS) $(INCLUDES) SparserSpark.cpp

SparserSpark.cpp: target/classes/SparserSpark.class
	javah -cp target/classes SparserSpark

target/classes/SparserSpark.class:
	mvn package

clean:
	mvn clean && rm -rf *.o *.class $(TARGET)

