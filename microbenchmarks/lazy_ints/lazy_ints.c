#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <assert.h>
#include <time.h>
#include <limits.h>

#include "common.h"

const int thres = 100000;

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

int main() {
    const char *filename = "../data/numbers.csv";
    double a = sum_data_standard(filename);
    double b = sum_data_lazy(filename);

    printf("Speedup: %f\n", a / b);
 }
