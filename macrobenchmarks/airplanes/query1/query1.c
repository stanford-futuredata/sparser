#include <stdlib.h>
#include <stdio.h>

#include <time.h>

#include <string.h>

#include <immintrin.h>

#include "../common/parser.h"
#include "../../../common/common.h"

void baseline(const char *filename) {
    aircraft_t *data = NULL;

    // Don't count disk load time.
    char *raw = read_all(filename);

    time_start();
    int length = parse(raw, &data);
    double parse_time = time_stop();

    free(raw);

    time_start();
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

    double query_time = time_stop();
    free(data);

    printf("%d (parse %.3f, query %.3f, total %.3f)\n", count, parse_time, query_time, parse_time+query_time);
}


// Schema:
/*
    AIRCRAFT_ID = 0,
    TAIL_NUMBER,
    AIRCRAFT_MODEL,
    AIRLINE,
    STATUS,
    CREATION_DATE,
    MOD_DATE,

    Each item separated by a \n character.
*/

// Size of a single vector.
#define VECSIZE 32

void fast(const char *filename) {

    // Don't count disk load time.
    char *raw = read_all(filename);

    // Current line.
    char *line = raw;
    // Current token.
    char *token = raw;

    // Finds newline characters.
    __m256i line_seeker = _mm256_set1_epi8('\n');
    // Finds delimiting characters.
    __m256i delimiter = _mm256_set1_epi8(',');

    int length = 86;

    for (int i = 0; i < length; i += VECSIZE) {
        __m256i word = _mm256_load_si256((__m256i const *)(raw + i));
        // Last iteration - mask out bytes past the end of the input
        if (i + VECSIZE > length) {
            // mask out unused "out of bounds" bytes.
            // This is slow...optimize.
            __m256i eraser = _mm256_cmpeq_epi8(line_seeker, line_seeker);
            for (int j = 0; j < i + VECSIZE - length; j++) {
                eraser = _mm256_insert_epi8(eraser, 0, VECSIZE - j - 1);
            }
            word = _mm256_and_si256(word, eraser);
        }

        __m256i mask = _mm256_cmpeq_epi8(word, line_seeker);
        int imask = _mm256_movemask_epi8(mask);
        while (imask) {
            int idx = ffs(imask) - 1;
            raw[idx + i] = '\0';
            printf("line %ld -> %d: %s\n", line - raw, idx + i, line);

            // Process `line` here. Length of the line is (idx + i) - (line - raw).

            line = raw + i + idx + 1;
            imask &= ~(1 << idx);
        }
    }
}

int main() {
    //baseline("../data/airplanes_big.csv");
    fast("../data/single.csv");
}
