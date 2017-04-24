#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <assert.h>
#include <time.h>
#include <limits.h>

#define THRES 10000000
#define THRES_STRLEN 3

long sum_data_standard(const char *filename) {
    const unsigned SIZE = 4096;
    static char buf[SIZE];

    int result = 0;
    FILE *f = fopen(filename, "r");
    assert(f);

    while (fgets(buf, SIZE, f)) {
        char *c;
        long l = atoi(buf);
        if (l == THRES) {
            result += l;
        }
    }
    fclose(f);
    return result;
}

long sum_data_lazy(const char *filename) {
    const unsigned SIZE = 4096;
    static char buf[SIZE];

    int result = 0;
    FILE *f = fopen(filename, "r");
    assert(f);

    char *b;
    asprintf(&b, "%d", THRES);
    // comparator. THRES must be exactly 8 characters here.
    long comp = *(long *)b;

    while (fgets(buf, SIZE, f)) {
        if (comp == *(long *)buf) {
            result += THRES;
        }
    }
    fclose(f);
    return result;
}

int main() {
    const char *filename = "data.csv";
    time_t start, end;

    start = clock();
    long result2 = sum_data_lazy(filename);
    end = clock();
    double cpu_time_used_2 = ((double) (end - start)) / CLOCKS_PER_SEC;

    start = clock();
    long result = sum_data_standard(filename);
    end = clock();
    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%ld (%.3f seconds)\n", result, cpu_time_used);

    printf("%ld == %ld\n", result, result2);	
    assert(result == result2);

    printf("%ld (%.3f seconds)\n", result2, cpu_time_used_2);

    printf("%.3fx speedup with lazy\n", cpu_time_used / cpu_time_used_2);
}
