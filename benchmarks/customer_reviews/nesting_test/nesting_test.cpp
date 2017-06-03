// rapidjson/example/simpledom/simpledom.cpp`
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"

#include <immintrin.h>

#include <iostream>

#include "common.h"

using namespace rapidjson;

#define VECSIZE 32

// The input.
const char *json_input = "{\"nested\": { \"overall\": 2.0 }, \"nested2\": { \"overall\": 3.0 }, \"overall\": 1.0 }";
const unsigned iterations = 1;

// Query: Average of root.json

double lazy_8byte() {
    int doc_index = 0;
    double score_average = 0.0;
    double total_time = 0.0;

    const char *key = "\"overall\"";
    const size_t keylen = strlen(key);
    const size_t input_length = strlen(json_input);

    __m256i obrackets = _mm256_set1_epi8('{');
    __m256i cbrackets = _mm256_set1_epi8('}');
    __m256i quotes = _mm256_set1_epi8('"');

    for (int i = 0; i < iterations; i++) {
        bench_timer_t s = time_start();

        char *ptr = (char *)json_input;
        // measures the object depth.
        int depth = 0;
        bool in_string = false;

        for (int i = 0; i < input_length; i += 32) {
            __m256i inp = _mm256_load_si256((__m256i *)(ptr + i));
            if (i + 32 > input_length) {
                // mask out unused "out of bounds" bytes.
                // This is slow...optimize.
                __m256i eraser = _mm256_cmpeq_epi8(inp, inp);
                for (int j = 0; j < i + 32 - input_length; j++) {
                    eraser = _mm256_insert_epi8(eraser, 0, 32 - j - 1);
                }
                inp = _mm256_and_si256(inp, eraser);
            }

            unsigned obs_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(inp, obrackets));
            unsigned cbs_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(inp, cbrackets));
            unsigned qbs_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(inp, quotes));

            // TODO Need to do something like a merge in a merge sort here; take the minimum of the
            // three, update state, and continue for each set bit. If depth == 1 and !is_string,
            // check for the key we're looking for. Should we also look for commas or something?
            while(obs_mask) {
                int idx = ffs(obs_mask) - 1;
                obs_mask &= ~(1 << idx);
            }

            while(cbs_mask) {
                int idx = ffs(cbs_mask) - 1;
                cbs_mask &= ~(1 << idx);
            }

            while(qbs_mask) {
                int idx = ffs(qbs_mask) - 1;
                qbs_mask &= ~(1 << idx);
            }

        }

finished_json:
        total_time += time_stop(s);
    }

    printf("Average overall score: %f (%.3f seconds)\n", score_average / doc_index, total_time);
    return total_time;
}

double lazy_baseline() {
    int doc_index = 0;
    double score_average = 0.0;
    double total_time = 0.0;

    const char *key = "\"overall\"";
    const size_t keylen = strlen(key);

    for (int i = 0; i < iterations; i++) {
        bench_timer_t s = time_start();

        char *ptr = (char *)json_input;
        // measures the object depth.
        int depth = 0;
        bool in_string = false;

        while(*ptr) {
            if (*ptr == '{') {
                depth++;
            } else if (*ptr == '}') {
                depth--;
            } else if (!in_string && depth == 1 && strncmp(ptr, key, keylen) == 0) {
                // Found it!
                ptr += keylen;
                for (; *ptr == ':' || *ptr == ' '; ptr++);
                score_average += strtof(ptr, NULL);
                doc_index++;
                goto finished_json;
            }

            // Check if we're entering a string.
            if (*ptr == '"') {
                in_string = !in_string;
            }
            ptr++;
        }

finished_json:
        total_time += time_stop(s);
    }

    printf("Average overall score: %f (%.3f seconds)\n", score_average / doc_index, total_time);
    return total_time;
}

double baseline_rapidjson() {

    int doc_index = 0;
    double score_average = 0.0;
    double total_time = 0.0;

    for (int i = 0; i < iterations; i++) {
        bench_timer_t s = time_start();
        Document d;
        d.Parse(json_input);

        assert(!d.HasParseError());

        score_average += d["overall"].GetDouble();
        doc_index++;

        total_time += time_stop(s);
    }

    printf("Average overall score: %f (%.3f seconds)\n", score_average / doc_index, total_time);
    return total_time;
}

int main() {
    double a = baseline_rapidjson();
    double b = lazy_8byte();
    printf("Speedup: %.3f\n", a / b);
}
