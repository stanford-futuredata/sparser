#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <assert.h>
#include <time.h>
#include <limits.h>

#include <immintrin.h>

#include "common.h"

const int thres = 1000000;
const char *thres_str = "1000000";

double sum_data_standard(const char *filename) {
    char *data = read_all(filename);
    char *token;

    long result = 0;

    time_start();
    while((token = strsep(&data, "\n")) != NULL) {
        long l = atoi(token);
        if (l < thres) {
            result += l;
        }
    }

    double time = time_stop();
    printf("%.3f seconds\n", time);
    printf("%ld\n", result);

    free(data);

    return time;
}

double sum_data_lazy(const char *filename) {

    char *data = read_all(filename);
    char *token;

    long result = 0;

    time_start();
    while((token = strsep(&data, "\n")) != NULL) {
        register char *p = token;
        register int k = 0;
        while (*p) {
            k = (k << 3) + (k << 1) + (*p) - '0';
            if (k >= thres) {
                goto next_iter;
            }
            p++;
        }
        result += k;
next_iter:
        ;
    }

    double time = time_stop();
    printf("%.3f seconds\n", time);
    printf("%ld\n", result);

    free(data);

    return time;
}

double sum_data_exact_match(const char *filename) {
    char *data = read_all(filename);
    char *token;

    long result = 0;

    time_start();
    while((token = strsep(&data, "\n")) != NULL) {
        long l = atoi(token);
        if (l == thres) {
            result += l;
        }
    }

    double time = time_stop();
    printf("%.3f seconds\n", time);
    printf("%ld\n", result);

    free(data);

    return time;
}

double sum_data_string_compare(const char *filename) {

    char *data = read_all(filename);
    char *token;

    long result = 0;

    unsigned len = strlen(thres_str);
    len *= 8;
    printf("%u\n", len);
    uint64_t mask = (1UL << len) - 1;
    printf("mask: %lld\n", mask);

    time_start();
    while((token = strsep(&data, "\n")) != NULL) {
        // TODO generalize to > 8 character integers.
        unsigned long x = *(unsigned long *)token;
        unsigned long target = *(unsigned long *)thres_str;
        if ((x & mask) == (target & mask)) {
            result+=thres;
        }
    }

    double time = time_stop();
    printf("%.3f seconds\n", time);
    printf("%ld\n", result);

    free(data);

    return time;
}

int main() {
    const char *filename_a = "../data/numbers.csv";
    const char *filename_b = "../data/100000s.csv";
    
    // Compare a < predicate and abort if we know we'll fail
    //double a = sum_data_standard(filename_a);
    //double b = sum_data_lazy(filename_a);
    //printf("Speedup: %f\n", a / b);

    // Compare exact match without parsing.
    double c = sum_data_exact_match(filename_b);
    double d = sum_data_string_compare(filename_b);
    printf("Speedup: %f\n", c / d);
 }
