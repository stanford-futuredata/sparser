#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <assert.h>
#include <time.h>
#include <limits.h>

#include <immintrin.h>

// Strategy 1: Parse using a "parser" into a buffer, and then 
// add everything up.

int parse_data(const char *filename, int **ret) {
    const unsigned SIZE = 4096;
    static char buf[SIZE];

    FILE *f = fopen(filename, "r");
    assert(f);

    unsigned capacity = 16;
    unsigned length = 0;
    int *data = (int *)malloc(sizeof(int) * capacity);

    while (fgets(buf, SIZE, f)) {
        int l = atoi(buf);
        if (length >= capacity) {
            capacity *= 2;
            data = (int *)realloc(data, sizeof(int) * capacity);
        }
        data[length] = l;
        length++;
    }

    fclose(f);
    *ret = data;
    return length;
}

int strategy1(const char *filename) {
    int *data;
    int result = 0;
    int length = parse_data(filename, &data);
    for (int i = 0; i < length; i++) {
        result += data[i];
    }
    free(data);
    return result;
}

//
// Strategy 2: Push the parsing inline, but no vectorization.

int strategy2(const char *filename) {
    const unsigned SIZE = 4096;
    static char buf[SIZE];

    int result = 0;
    FILE *f = fopen(filename, "r");
    assert(f);

    while (fgets(buf, SIZE, f)) {
        int l = atoi(buf);
        result += l;
    }
    fclose(f);
    return result;
}

//
// Strategy 3: Push the parsing inline, with partial buffering and vectorization.

int strategy3(const char *filename) {
    const int VECBUFSZ = (2 << 12);
    const unsigned SIZE = 4096;
    static char buf[SIZE];

    // tunable buffer size - ideally, we want it to be the L1 cache size?
    int vector_buffer[VECBUFSZ];
    int vbi = 0;

    int result = 0;
    FILE *f = fopen(filename, "r");
    assert(f);

    __m256i resv = _mm256_setzero_si256();
    while (fgets(buf, SIZE, f)) {
        if (vbi == VECBUFSZ) {
            for (int i = 0; i < VECBUFSZ; i += 8) {
                __m256i a = _mm256_loadu_si256((__m256i const *)(vector_buffer + i));
                resv = _mm256_add_epi32(a, resv);
            }
            vbi = 0;
        }

        int l = atoi(buf);
        vector_buffer[vbi] = l;
        vbi++;
    }

    // Fringe.
    int i = 0;
    for (; i < vbi; i+=8) {
        __m256i a = _mm256_loadu_si256((__m256i const *)(vector_buffer + i));
        resv = _mm256_add_epi32(a, resv);
    }
    for (; i < vbi; i++) {
        result += vector_buffer[i];
    }

    int x[8];
    _mm256_store_si256((__m256i *)x, resv);
    for (i = 0; i < 8; i++) {
        result += x[i];
    }


    fclose(f);
    return result;
}

int main() {
    const char *filename = "data.csv";
    time_t start, end;

    // Strategy 1
    start = clock();
    int result = strategy1(filename);
    end = clock();
    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%d (%.3f seconds)\n", result, cpu_time_used);

    // Strategy 2
    start = clock();
    int result2 = strategy2(filename);
    end = clock();
    double cpu_time_used_2 = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%d (%.3f seconds)\n", result2, cpu_time_used_2);

    // Strategy 3
    start = clock();
    int result3 = strategy3(filename);
    end = clock();
    double cpu_time_used_3 = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%d (%.3f seconds)\n", result3, cpu_time_used_3);

    printf("%.3fx speedup with S2\n", cpu_time_used / cpu_time_used_2);
    printf("%.3fx speedup with S3\n", cpu_time_used / cpu_time_used_3);
}
