#include <stdlib.h>
#include <stdio.h>

#include <time.h>

#include <string.h>

#include <immintrin.h>

#include "parser.h"
#include "common.h"

double baseline(const char *filename) {
    aircraft_t *data = NULL;

    char *raw = NULL;
    // Don't count disk load time.
    // Returns number of bytes in the buffer.
    read_all(filename, &raw);

    bench_timer_t s = time_start();
    int length = parse(raw, &data);
    double parse_time = time_stop(s);

    free(raw);

    s = time_start();
    const char *airlines[] = {
        "United",
        "American",
        "Delta",
    };
     
    const size_t strsizes[] = {
        strlen(airlines[0]),
        strlen(airlines[1]),
        strlen(airlines[2]),
    };

    int count = 0;
    for (int i = 0; i < length; i++) {
        for(int j = 0; j < 3; j++) {
            const char *airline = airlines[j];
            const size_t strsize = strsizes[j];
            if (strncmp(data[i].airline, airline, strsize) == 0) {
                count++;
                break;
            }
        }
    }

    double query_time = time_stop(s);
    free(data);

    printf("%d (parse %.3f, query %.3f, total %.3f)\n", count, parse_time, query_time, parse_time + query_time);
    return parse_time + query_time;
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

// If true, uses a single packed vector compare to check all predicates.
#define PACKED_COMPARE  1

double fast(const char *filename) {

    // The  final result.
    int count = 0;

    char *raw = NULL;
    // Don't count disk load time.
    // Returns number of bytes in the buffer.
    long length = read_all(filename, &raw);

    bench_timer_t s = time_start();

    const char *airlines[] = {
        "United",
        "American",
        "Delta",
    };
     
    const size_t strsizes[] = {
        strlen(airlines[0]),
        strlen(airlines[1]),
        strlen(airlines[2]),
    };

#if PACKED_COMPARE
    // Possible predicates start at byte 0, 3, 5
    const char *pred_prefixes_str = "UniAmDel";
    unsigned long pred_prefixes = *((unsigned long *)pred_prefixes_str);
#endif

    // Current line.
    char *line = raw;
    // Finds newline characters.
    __m256i line_seeker = _mm256_set1_epi8('\n');
    for (long i = 0; i < length; i += VECSIZE) {
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
            int passing = 0;
            
            // Current token.
            char *token = line;
            // Finds delimiting characters.
            __m256i delimiter = _mm256_set1_epi8(',');
            int line_length = (idx + i) - (line - raw);

            for (long j = 0; j < line_length; j += VECSIZE) {
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
                    size_t token_size = (line_idx + j) - (token - line);
                    if (token_index == AIRLINE) {
#if PACKED_COMPARE
                        unsigned long token_long = *((unsigned long *)token);
                        // Copy first three bytes, then first two bytes, then first three bytes
                        // again.
                        unsigned long first_three = token_long & 0xffffff;
                        unsigned long first_two = (token_long & 0xffff) << 3*8;
                        unsigned long last_three = (first_three << 5*8);
                        unsigned long to_compare = first_three | first_two | last_three;

                        unsigned long predmask = (pred_prefixes & to_compare);
                        if ((predmask & 0xffffffL) == first_three && strncmp(token, airlines[0], strsizes[0]) == 0) {
                            count++;
                        } 
                        else if ((predmask & 0xffff000000L) == first_two && strncmp(token, airlines[1], strsizes[1]) == 0) {
                            count++;
                        }
                        else if ((predmask & 0xffffff0000000000L) == last_three && strncmp(token, airlines[2], strsizes[2]) == 0) {
                            count++;
                        }
#else
                        for(int j = 0; j < 3; j++) {
                            const char *airline = airlines[j];
                            const size_t strsize = strsizes[j];
                            if (strncmp(token, airline, strsize) == 0) {
                                count++;
                                break;
                            }
                        }
#endif
                    }

                    token = line + j + line_idx + 1;
                    line_imask &= ~(1 << line_idx);
                    token_index++;

                    // Some simple short circuiting. Comment this out to disable and parse all the
                    // data unconditionally.
                    if (token_index > AIRLINE) {
                        goto end_token_processing;
                    }
                }
            }

            // The last token, goes to the end of the buffer.
#if PACKED_COMPARE
#else
            for(int j = 0; j < 3; j++) {
                const char *airline = airlines[j];
                const size_t strsize = strsizes[j];
                if (strncmp(token, airline, strsize) == 0) {
                    count++;
                    break;
                }
            }
#endif



end_token_processing:

            // End Token Processing. 
            ////////////

            line = raw + i + idx + 1;
            imask &= ~(1 << idx);
        }

        // TODO Special processing for the last line?
    }

    double total = time_stop(s);
    free(raw);

    printf("%d (parse + query: %.3f)\n", count, total);
    return total;
}

int main() {
    //double a = baseline(path_for_data("airplanes_big.csv"));
    double b = fast(path_for_data("airplanes_big.csv"));
    double a = 1.0;

    printf("Speedup: %.3f\n", a / b);
}
