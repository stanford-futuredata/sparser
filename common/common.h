#ifndef _COMMON_H_
#define _COMMON_H_

#include <assert.h>
#ifdef USE_HDFS
#include <hdfs/hdfs.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define USE_WALL_TIME
#ifdef USE_WALL_TIME
#include <sys/time.h>
#else
#include <time.h>
#endif

// Handle for timing.
#ifdef USE_WALL_TIME
typedef struct timeval bench_timer_t;

/** Starts the clock for a benchmark. */
inline bench_timer_t time_start() {
    bench_timer_t t;
    gettimeofday(&t, NULL);
    return t;
}

/** Stops the clock and returns time elapsed in seconds.
 * Throws an error if time__start() was not called first.
 * */
inline double time_stop(bench_timer_t start) {
    bench_timer_t end;
    bench_timer_t diff;
    gettimeofday(&end, NULL);
    timersub(&end, &start, &diff);
    return (double)diff.tv_sec + ((double)diff.tv_usec / 1000000.0);
}

#else
typedef clock_t bench_timer_t;

inline bench_timer_t time_start() {
    bench_timer_t t = clock();
    return t;
}

inline double time_stop(bench_timer_t start) {
    clock_t _end = clock();
    return ((double)(_end - start)) / CLOCKS_PER_SEC;
}
#endif

/** Returns a formatted string suitable for benchmark parsing. */
inline static char *benchmark_string(const char *name, double time) {
    static char buf[8192];
    snprintf(buf, sizeof(buf), "%s: %f", name, time);
    return buf;
}

/** Returns a path for the given data file. */
inline static char *path_for_data(const char *datafile) {
    static char buf[8192];
    char *envvar = getenv("SPARSER_HOME");
    assert(envvar);
    snprintf(buf, sizeof(buf), "%s/benchmarks/datagen/_data/%s", envvar,
             datafile);
    return buf;
}

#ifdef USE_HDFS
extern hdfsFS fs;

/**
 * Reads the entire _local_ block of a file from HDFS. There isn't an explicit
 * API for reading only the local block, but we can get the start offset and the
 * number of bytes for the local block from Spark. `filename_uri` must be of the
 * form "hdfs://hostname/path/to/file"
 **/
long read_hdfs(const char *filename_uri, char **buf, unsigned long start,
               unsigned long length) {
    // Extract file path from filename_uri, skip over "hdfs://hostname"
    char *filename = (char *)filename_uri + 7;
    while (*filename != '/') {
        ++filename;
    }

    char *x = (char *)malloc(length + 1);
    char *buffer = x;

    hdfsFile fin = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
    int ret = hdfsSeek(fs, fin, start);
    unsigned long num_bytes_read = 0;
    unsigned long bytes_remaining = length;
    while (num_bytes_read < length) {
        // bench_timer_t t_start = time_start();
        const unsigned long done = hdfsRead(fs, fin, buffer, bytes_remaining);
        // printf("node: m, start: %lu, bytes_remaining: %lu, hdfsRead: %f\n",
        // start, bytes_remaining, time_stop(t_start));
        if (done <= 0) break;
        buffer += done;
        num_bytes_read += done;
        bytes_remaining -= done;
    }

    ret = hdfsCloseFile(fs, fin);
    x[length] = '\0';
    *buf = x;

    return length + 1;
}
#endif

/**
 * Reads chunk of local file `filename` into memory, used in Spark benchmarking.
 * `filename_uri` must be of the form "file:///path/to/file"
 **/
long read_local(const char *filename_uri, char **buf, unsigned long start,
                unsigned long length) {
    // Extract file path from filename_uri, skip over "file://"
    char *filename = (char *)filename_uri + 7;

    FILE *f = fopen(filename, "r");
    if (!f) {
        fprintf(stderr, "%s: ", filename);
        perror("read_local");
        exit(1);
    }

    fseek(f, start, SEEK_SET);
    char *string = (char *)malloc(length + 1);
    fread(string, length, 1, f);
    fclose(f);

    string[length] = '\0';

    *buf = string;
    return length + 1;
}

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

#endif
