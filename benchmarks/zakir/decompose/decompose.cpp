#include "decompose.h"
#include "queries.h"

#include <string.h>
#include <stdlib.h>

// The length of produced substrings.
#define REGSZ 4

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
