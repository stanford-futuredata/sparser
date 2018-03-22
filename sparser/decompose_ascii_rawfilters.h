/** 
 *
 * decompose_ascii_rawfilters.h
 *
 * Generates substring search raw filters for ASCII-based formats such as JSON
 * and CSV.
 *
 * This file is format-specific: different formats will have different
 * ways of generating candidate RFs.
 */

#ifndef _DECOMPOSE_H_
#define _DECOMPOSE_H_

#include <string.h>
#include <stdlib.h>

// The length of produced substrings.
#define REGSZ 4

typedef struct ascii_rawfilters {
  // The ascii_rawfilters strings. Each pointer points into region.
	const char **strings;
	// The source of the string.
	const int *sources;
	const int *lens;
	// Region where strings are allocated.
	char *region;
	// Number of strings created.
	int num_strings;
} ascii_rawfilters_t;

// Decomposes each string into substrings of length REGSZ or less as search tokens.
ascii_rawfilters_t decompose(const char **predicates, int num_predicates) {

	int num_ascii_rawfilters = 0;
	int region_bytes = 0;

	for (int i = 0; i < num_predicates; i++) {
		int len = (int)strlen(predicates[i]);

		// How many REGSZ-length substrings are possible from this string?
		int possible_substrings = len - REGSZ > 0 ? len - REGSZ + 1: 1;
		// Include the full string in the count.
		num_ascii_rawfilters += possible_substrings + 1;

		region_bytes += (possible_substrings * 5);
	}

	const char **result = (const char **)malloc(sizeof(char *) * num_ascii_rawfilters);
	int *sources = (int *)malloc(sizeof(int) * num_ascii_rawfilters);
	int *lens = (int *)malloc(sizeof(int) * num_ascii_rawfilters);
	char *region = (char *)malloc(sizeof(char) * region_bytes); 

	// index into result.
	int i = 0;
	// pointer into region.
	char *region_ptr = region;

	for (int j = 0; j < num_predicates; j++) {
		// Add the first string.
		result[i] = predicates[j];	
		lens[i] = strlen(predicates[j]);
		sources[i] = j;
		i++;
		
		int pred_length = strlen(predicates[j]);
		for (int start = 0; start <= pred_length - REGSZ; start++) {

			if (pred_length == REGSZ && start == 0) continue;

			memcpy(region_ptr, predicates[j] + start, REGSZ);
			region_ptr[REGSZ] = '\0';

			result[i] = region_ptr;
			sources[i] = j;
			lens[i] = REGSZ;

			region_ptr += 5;
			i++;
		}
	}

	ascii_rawfilters_t d;
	d.strings = result;
	d.sources = sources;
	d.lens = lens;
	d.region = region;
	d.num_strings = i;

	return d; 
}

void free_ascii_rawfilters(ascii_rawfilters_t *d) {
  free((void *)d->strings);
  free((void *)d->sources);
  free((void *)d->lens);
  free((void *)d->region);
}

#endif
