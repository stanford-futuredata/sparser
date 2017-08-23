CC=g++
CFLAGS=-c -fPIC
LDFLAGS=-shared -Wl,-soname,sparser.so
INCLUDES=-I/usr/lib/jvm/java-8-openjdk-amd64/include -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux
TARGET=libsparser.so

.PHONY: all clean

all:	$(TARGET)

$(TARGET): SparserSpark.o
	$(CC) $(INCLUDES) -o $(TARGET) $(LDFLAGS) SparserSpark.o

SparserSpark.o: SparserSpark.c
	$(CC) $(CFLAGS) $(INCLUDES) SparserSpark.c

SparserSpark.c: target/classes/SparserSpark.class
	javah -cp target/classes SparserSpark

target/classes/SparserSpark.class:
	mvn package

clean:
	mvn clean && rm -rf *.o *.class $(TARGET)
