#include <stdlib.h>
#include <stdio.h>

#include <time.h>

#include <string.h>

#include <immintrin.h>
#include <arpa/inet.h>

#include "common.h"

#define VECSZ 32

inline int check(__m256i xs, const char *address) {
    int count = 0;

    // For debugging.
    char buf[VECSZ + 1];
    buf[VECSZ] = 0;

    __m256i val = _mm256_loadu_si256((__m256i const *)(address));

    int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(xs, val));
    if (mask) {
        int prev = ffs(mask) - 1;
        while ((prev) % 2 != 0 && mask) {
            mask &= ~(1 << prev);
            prev = ffs(mask) - 1;
        }

        while (mask) {
            int index = ffs(mask) - 1;
            // check if characters are adjacent.
            if (index - prev == 1 && index % 2 == 1) {
                count++;
                // memcpy(buf, address, VECSZ);
                // printf("found \"%s\" in string: %s\n", search_str, (char *)buf);
            }
            mask &= ~(1 << index);
        }
    }
    return count;
}

// Sums the bytes in a file. Baseline for read speed.
double baseline_sum(const char *filename) {
    char *raw = NULL;

    // Don't count disk load time.
    // Returns number of bytes in the buffer.
    long size = read_all(filename, &raw);

    bench_timer_t s = time_start();

    long count = 0;

    __m256i sum = _mm256_setzero_si256();

    for (size_t offset = 0; offset < size; offset += VECSZ) {
        __m256i val = _mm256_loadu_si256((__m256i const *)(raw + offset));
        sum = _mm256_add_epi64(val, sum);
    }

    long buf[4];
    _mm256_storeu_si256((__m256i *)buf, sum);
    long final = 0;
    for (int i = 0; i < 4; i++) {
        final += buf[i];
    }

    double parse_time = time_stop(s);

    printf("%ld\n", final);
    printf("%f seconds\n", parse_time);

    free(raw);
    return 0.0;
}

double baseline(const char *filename) {
    char *raw = NULL;
    // Don't count disk load time.
    // Returns number of bytes in the buffer.
    long size = read_all(filename, &raw);

    bench_timer_t s = time_start();

    long count = 0;

    const unsigned short z1 = 0;

    const char *search_str = "fo";
    uint16_t search = *((uint16_t *)search_str);
    printf("%s 0x%x\n", search_str, search);

    __m256i xs = _mm256_set1_epi16(search);

    char buf[33];
    buf[32] = 0;
    _mm256_storeu_si256((__m256i *)buf, xs);
    printf("%s\n", buf);

    for (size_t offset = 0; offset < size; offset += VECSZ) {
        count += check(xs, raw + offset);
        count += check(xs, raw + offset + 1);
    }

    double parse_time = time_stop(s);

    printf("Number of xs: %ld (%.3f%% of the input)\n", count, 100.0 * (double)(count * strlen(search_str)) / (double)size);
    printf("%f seconds\n", parse_time);

    free(raw);

    return 0.0;
}

int main() {
    const char *filename = path_for_data("tweets.json");
    baseline_sum(filename);
    baseline(filename);

    return 0;
}
