#ifndef _COMMON_H_
#define _COMMON_H_

#include <assert.h>
#ifdef USE_HDFS
#include <hdfs/hdfs.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Handle for timing.
typedef clock_t bench_timer_t;

/** Returns a formatted string suitable for benchmark parsing. */
static char *benchmark_string(const char *name, double time) {
    static char buf[8192];
    snprintf(buf, sizeof(buf), "%s: %f", name, time);
    return buf;
}

/** Returns a path for the given data file. */
static char *path_for_data(const char *datafile) {
    static char buf[8192];
    char *envvar = getenv("SPARSER_HOME");
    assert(envvar);
    snprintf(buf, sizeof(buf), "%s/benchmarks/datagen/_data/%s", envvar,
             datafile);
    return buf;
}

#ifdef USE_HDFS
/**
 * Reads the entire _local_ block of a file from HDFS. There isn't an explicit
 * API for reading only the local block, but we can get the start offset and the
 * number of bytes for the local block from Spark. `filename_uri` must be of the
 * form "hdfs://hostname/path/to/file"
 **/
long read_all_hdfs(const char *filename_uri, char **buf, long start,
                   long length) {
    printf("Start: %lu\n", start);
    printf("Length: %lu\n", length);
    // must start with "hdfs://"
    if (strncmp("hdfs://", filename_uri, 7)) {
        printf("filename_uri %s does not start with 'hdfs://'\n", filename_uri);
        abort();
    }
    // Extract hostname and file path from filename_uri
    char *filename = (char *)filename_uri + 7;
    while (*filename != '/') {
        ++filename;
    }
    const unsigned int hostname_length = filename - (filename_uri + 7);
    char *hostname = (char *)malloc(hostname_length + 1);
    strncpy(hostname, filename_uri + 7, hostname_length);
    hostname[hostname_length] = '\0';

    // connect to NameNode
    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, hostname);
    // TODO: don't hardcode HDFS port
    hdfsBuilderSetNameNodePort(builder, 8020);
    hdfsFS fs = hdfsBuilderConnect(builder);

    char *x = (char *)malloc(length + 1);
    char *buffer = x;

    hdfsFile fin = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
    int ret = hdfsSeek(fs, fin, start);
    unsigned long num_bytes_read = 0;
    unsigned long bytes_remaining = length;
    while (num_bytes_read < length) {
      const unsigned long done = hdfsRead(fs, fin, buffer, bytes_remaining);
      if (done <= 0) break;
      buffer += done;
      num_bytes_read += done;
      bytes_remaining -= done;
    }

    ret = hdfsCloseFile(fs, fin);
    x[length] = '\0';
    *buf = x;

    hdfsFreeBuilder(builder);
    free(hostname);

    return length + 1;
}
#endif

/** Reads the entire file filename into memory. */
long read_all(const char *filename, char **buf) {
    FILE *f = fopen(filename, "r");
    if (!f) {
        fprintf(stderr, "%s: ", filename);
        perror("read_all");
        exit(1);
    }

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);  // same as rewind(f);

    char *string = (char *)malloc(fsize + 1);
    fread(string, fsize, 1, f);
    fclose(f);

    string[fsize] = 0;

    *buf = string;
    return fsize + 1;
}

/** Starts the clock for a benchmark. */
bench_timer_t time_start() {
    bench_timer_t t = clock();
    return t;
}

/** Stops the clock and returns time elapsed in seconds.
 * Throws an error if time__start() was not called first.
 * */
double time_stop(bench_timer_t start) {
    clock_t _end = clock();
    return ((double)(_end - start)) / CLOCKS_PER_SEC;
}

#endif
