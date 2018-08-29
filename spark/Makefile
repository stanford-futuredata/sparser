UNAME := $(shell uname -s)

CC=clang++
CFLAGS=-c -std=c++11 -fPIC -march=native -O3 -Wall -DUSE_HDFS
LDFLAGS=-shared -Wl
LIBRARIES=-lz -L../common/libhdfs/lib -lhdfs3
INCLUDES=-I$(JAVA_HOME)/include \
	 -I ../sparser \
	 -I ../json \
	 -I ../json/rapidjson/include
	 #-I libhdfs3/include

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

$(TARGET): edu_stanford_sparser_SparserNative.o
	$(CC) $(INCLUDES) -o $(TARGET) $(LIBRARIES) $(LDFLAGS) edu_stanford_sparser_SparserNative.o

edu_stanford_sparser_SparserNative.o: edu_stanford_sparser_SparserNative.cpp
	$(CC) $(CFLAGS) $(INCLUDES) edu_stanford_sparser_SparserNative.cpp

edu_stanford_sparser_SparserNative.cpp: target/classes/edu/stanford/sparser/SparserNative.class
	javah -cp target/classes edu.stanford.sparser.SparserNative

target/classes/edu/stanford/sparser/SparserNative.class:
	mvn package

clean:
	mvn clean && rm -rf *.o *.class $(TARGET)

