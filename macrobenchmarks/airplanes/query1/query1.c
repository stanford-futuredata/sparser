#include <stdlib.h>
#include <stdio.h>

#include <time.h>

#include <string.h>

#include <immintrin.h>

#include "../common/parser.h"
#include "../../../common/common.h"

void baseline(const char *filename) {
    aircraft_t *data = NULL;

    char *raw = NULL;
    // Don't count disk load time.
    // Returns number of bytes in the buffer.
    read_all(filename, &raw);

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

    printf("%d (parse %.3f, query %.3f, total %.3f)\n", count, parse_time, query_time, parse_time + query_time);
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

    // The  final result.
    int count = 0;

    char *raw = NULL;
    // Don't count disk load time.
    // Returns number of bytes in the buffer.
    int length = read_all(filename, &raw);

    time_start();

    const char *model = "B747-400";
    const char *airline = "United Airlines";
    size_t len_airline = strlen(airline);
    size_t len_model = strlen(model);

    // Current line.
    char *line = raw;
    // Finds newline characters.
    __m256i line_seeker = _mm256_set1_epi8('\n');
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
            //printf("line %ld -> %d: %s\n", line - raw, idx + i, line);

            // Process `line` here. Length of the line is (idx + i) - (line - raw).
            
            /////////////
            // Begin Token Processing. 
            
            // Token index being processed.
            int token_index = 0;
            int passing = 1;
            
            // Current token.
            char *token = line;
            // Finds delimiting characters.
            __m256i delimiter = _mm256_set1_epi8(',');
            int line_length = (idx + i) - (line - raw);

            for (int j = 0; j < line_length; j += VECSIZE) {
                __m256i line_word = _mm256_load_si256((__m256i const *)(line + j));
                // Last iteration - mask out bytes past the end of the input
                if (j + VECSIZE > line_length) {
                    // mask out unused "out of bounds" bytes.
                    // This is slow...optimize.
                    __m256i line_eraser = _mm256_cmpeq_epi8(delimiter, delimiter);
                    for (int k = 0; k < j + VECSIZE - line_length; k++) {
                        //printf("masking top %d byte\n", VECSIZE - k - 1);
                        line_eraser = _mm256_insert_epi8(line_eraser, 0, VECSIZE - k - 1);
                    }
                    line_word = _mm256_and_si256(line_word, line_eraser);
                }

                __m256i line_mask = _mm256_cmpeq_epi8(line_word, delimiter);
                int line_imask = _mm256_movemask_epi8(line_mask);
                while (line_imask) {
                    int line_idx = ffs(line_imask) - 1;
                    line[line_idx + j] = '\0';
                    //printf("%d\n", token_index);
                    //printf("token %ld -> %d: %s\n", token - line, line_idx + j, token);

                    // Process `token` here. Length of the line is (line_idx + j) - (token - line).
                    switch (token_index) {
                        case AIRCRAFT_MODEL:
                            if (strncmp(model, token, len_model) != 0) {
                                passing = 0;
                            }
                            break;
                        case AIRLINE:
                            if (strncmp(airline, token, len_airline) != 0) {
                                passing = 0;
                            }
                            break;
                        default:
                            break;
                    }

                    token = line + j + line_idx + 1;
                    line_imask &= ~(1 << line_idx);
                    token_index++;
                }
            }

            // The last token, goes to the end of the buffer.
            switch (token_index) {
                case AIRCRAFT_MODEL:
                    if (strncmp(model, token, len_model) > 0) {
                        passing = 0;
                    }
                    break;
                case AIRLINE:
                    if (strncmp(airline, token, len_airline) > 0) {
                        passing = 0;
                    }
                    break;
                default:
                    break;
            }


            if (passing == 1) {
                count++;
            }

            // End Token Processing. 
            ////////////

            line = raw + i + idx + 1;
            imask &= ~(1 << idx);
        }

        // TODO Special processing for the last line?
    }


    double total = time_stop();
    printf("%d (parse + query: %.3f)\n", count, total);
}

int main() {
    baseline("../data/airplanes_small.csv");
    fast("../data/airplanes_small.csv");
}
