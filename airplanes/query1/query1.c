#include <stdlib.h>
#include <stdio.h>

#include <time.h>

#include <string.h>

#include "../common/parser.h"

int baseline(const char *filename) {
    aircraft_t *data = NULL;
    int length = load(filename, &data);

    const char *model = "B747-400";
    const char *airline = "United Airlines";
    size_t len_airline = strlen(airline);
    size_t len_model = strlen(model);

    int count = 0;
    for (int i = 0; i < length; i++) {
        if (strncmp(model, data[i].aircraft_model, len_model) == 0 && strncmp(airline, data[i].airline, len_airline) == 0) {
            count += 1;
        }
    }
    
    free(data);
    return count;
}

int main() {
    clock_t start, end;
    start = clock();
    long result = baseline("../data/airplanes_big.csv");
    end = clock();
    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%ld (%.3f seconds)\n", result, cpu_time_used);
}
