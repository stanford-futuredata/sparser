#include <stdlib.h>
#include <stdio.h>

#include <time.h>

#include <string.h>

#include <immintrin.h>

#include "parser.h"
#include "common.h"

// Scans the input.
double baseline(const char *filename) {

    char *raw = NULL;
    long count = 0;
    size_t bytes = read_all(filename, &raw);

    size_t length = bytes / 8;
    long *as_words = (long *)raw;

    bench_timer_t s = time_start();

    __m256i total = _mm256_setzero_si256();
    // Reads each byte.
    for (int i  = 0; i + 4 < length; i += 4) {
        __m256i x = _mm256_loadu_si256((__m256i *)(as_words + i));
        total = _mm256_add_epi64(total, x);
    }

    double query_time = time_stop(s);

    long out[4];
    _mm256_storeu_si256((__m256i *)out, total);
    count = out[0];

    free(raw);

    printf("%ld (total %.3f)\n", count, query_time);
    return query_time;
}


int main() {
    baseline(path_for_data("airplanes_big.csv"));
}
