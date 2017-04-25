#ifndef _COMMON_H_
#define _COMMON_H_

/** Reads the entire file filename into memory. */
char *read_all(const char *filename);

/** Starts the clock for a benchmark. */
void time_start();

/** Stops the clock and returns time elapsed in seconds. 
 * Throws an error if time_start() was not called first.
 * */
double time_stop();

#endif
