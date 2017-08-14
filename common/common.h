#ifndef _COMMON_H_
#define _COMMON_H_

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <assert.h>

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
    if (!envvar) {
        envvar = "/Users/shoumikpalkar/work/sparser/";
    }

    snprintf(buf, sizeof(buf), "%s/benchmarks/datagen/_data/%s", envvar, datafile);
    return buf;
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
	fseek(f, 0, SEEK_SET);  //same as rewind(f);

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
    return ((double) (_end - start)) / CLOCKS_PER_SEC;
}

#endif
