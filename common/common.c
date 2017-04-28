#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <assert.h>

static clock_t start, end;
static int reset = 0;

/** Reads the entire file filename into memory. */
long read_all(const char *filename, char **buf) {
    FILE *f = fopen(filename, "r");

	fseek(f, 0, SEEK_END);
	long fsize = ftell(f);
	fseek(f, 0, SEEK_SET);  //same as rewind(f);

	char *string = malloc(fsize + 1);
	fread(string, fsize, 1, f);
	fclose(f);

	string[fsize] = 0;

    *buf = string;
	return fsize + 1;
}

/** Starts the clock for a benchmark. */
void time_start() {
	start = clock();
	reset = 1;
}

/** Stops the clock and returns time elapsed in seconds. 
 * Throws an error if time_start() was not called first.
 * */
double time_stop() {
	end = clock();
	assert(reset);
	reset = 0;
    return ((double) (end - start)) / CLOCKS_PER_SEC;
}
