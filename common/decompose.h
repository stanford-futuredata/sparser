#ifndef _DECOMPOSE_H_
#define _DECOMPOSE_H_

#include <string.h>
#include <stdlib.h>

// The length of produced substrings.
#define REGSZ 4

typedef struct decomposed {
  // The decomposed strings.
	const char **strings;
	// The source of the string.
	const int *sources;
	// Region where strings are allocated.
	char *region;
	// Number of strings created.
	int num_strings;
} decomposed_t;

// Decomposes each string into substrings of length REGSZ or less as search tokens.
decomposed_t decompose(const char **predicates, int num_predicates) {

	int num_decomposed = 0;
	int region_bytes = 0;

	for (int i = 0; i < num_predicates; i++) {
		int len = (int)strlen(predicates[i]);

		// How many REGSZ-length substrings are possible from this string?
		int possible_substrings = len - REGSZ > 0 ? len - REGSZ + 1: 1;
		// Include the full string in the count.
		num_decomposed += possible_substrings + 1;

		region_bytes += (possible_substrings * 5);
	}

	const char **result = (const char **)malloc(sizeof(char *) * num_decomposed);
	int *sources = (int *)malloc(sizeof(int) * num_decomposed);
	char *region = (char *)malloc(sizeof(char) * region_bytes); 

	// index into result.
	int i = 0;
	// pointer into region.
	char *region_ptr = region;

	for (int j = 0; j < num_predicates; j++) {
		// Add the first string.
		result[i] = predicates[j];	
		sources[i] = j;
		i++;
		
		int pred_length = strlen(predicates[j]);
		for (int start = 0; start <= pred_length - REGSZ; start++) {
			memcpy(region_ptr, predicates[j] + start, REGSZ);
			region_ptr[REGSZ] = '\0';

			result[i] = region_ptr;
			sources[i] = j;

			region_ptr += 5;
			i++;
		}
	}

	decomposed_t d;
	d.strings = result;
	d.sources = sources;
	d.region = region;
	d.num_strings = i;

	return d; 
}

#endif

#if 0 // For testing.
int main() {
	int count;
	const char **preds = sparser_zakir_query1(&count);

	decomposed_t d = decompose(preds, count);

	for (int i = 0; i < d.num_strings; i++) {
		printf("%s (source=%d)\n", d.strings[i], d.sources[i]);
	}
	printf("Produced %d strings\n", d.num_strings);
	free(d.region);
}
#endif
