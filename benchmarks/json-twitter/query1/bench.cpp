#include <stdlib.h>
#include <stdio.h>

#include <time.h>

#include <string.h>

#include <immintrin.h>
#include <arpa/inet.h>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"

#include "common.h"

using namespace rapidjson;

#define VECSZ 32

static char print_buffer[4096];

const char *search_str = "SAD!";

bool rapidjson_parse(const char *line);

static inline void RESET_PRINTER() {
    memset(print_buffer, 0, sizeof(print_buffer));
}

// Compares address with xs to find matches.
int check(__m256i xs, const char *address) {
    int count = 0;
    __m256i val = _mm256_loadu_si256((__m256i const *)(address));
    // change to epi8 for single bit comparison
    unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(xs, val));

    // Remove redundancy from movemask instruction.
    // Set to 0x11111111 for epi32, 0xf0f0f0f0f0 for epi16. No mask for byte-level comparator.
    mask &= 0x11111111;

    while (mask) {
        int index = ffs(mask) - 1;
        mask &= ~(1 << index);

        // for epi16 version
        count++;

        // for epi8 version
        /*
        // Odd offset - don't count it as a match.
        if (index % 2 == 1) {
            continue;
        }

        // Even offset - check the next bit.
        int next = ffs(mask) - 1;
        mask &= ~(1 << next);

        // match if consecutive bits are set.
        if (next - index == 1) {
            count++;
        }
        */
    }
    return count;
}

// Applies sparser + RapidJSON for a full parse if sparser returns a positive signal.
double baseline(const char *filename) {
    char *raw = NULL;

    // Don't count disk load time.
    // Returns number of bytes in the buffer.
    long size = read_all(filename, &raw);

    bench_timer_t s = time_start();

    // Tokens sparser matched.
    long sparser_matched = 1;
    // Number of records sparser chose to pass to the full parser.
    long sparser_records_passed = 1;
    // Number of records which actually matched using the full parser.
    long matching = 1;
    // Total number of documents.
    long doc_index = 1;

    // This is the string to search.
    // TODO - generalize this - the check function should be a function pointer set according to the number of bytes
    // we want to match.
    //
    // Currently set to 32-bit matching (so we should use .._epi32() everywhere).
    uint32_t search = *((uint32_t *)search_str);
    __m256i xs = _mm256_set1_epi32(search);
    __m256i newlines = _mm256_set1_epi8('\n');

    RESET_PRINTER();
    _mm256_storeu_si256((__m256i *)print_buffer, xs);
    printf("%s\n", print_buffer);

    for (size_t offset = 0; offset < size; offset += VECSZ) {
        int tokens_found = 0;

        /*
        // Probably don't want to load these over and over again...hopefully -O3 fixes it for us.
        __m256i val = _mm256_loadu_si256((__m256i const *)(raw + offset));
        __m256i mask = _mm256_cmpeq_epi8(val, newlines);
        int imask = _mm256_movemask_epi8(mask);
        if (imask) {
            int idx = ffs(imask) - 1;

            assert(raw[offset + idx] == '\n');

            // Index where the record starts.
            record_start = offset + idx;
            raw[record_start] = '\0';
            record_start++;
        }
        */

        // Fuzzy check
        tokens_found += check(xs, raw + offset);
        tokens_found += check(xs, raw + offset + 1);
        tokens_found += check(xs, raw + offset + 2);
        tokens_found += check(xs, raw + offset + 3);

        // Fuzzy check passed - use full parser to verify.
        if (tokens_found) {

            // Race forward to null-terminate so we can pass the token to the parser.
            // TODO use vectors
            int record_end = offset;
            for (; record_end < size && raw[record_end] != '\n'; record_end++);
            raw[record_end] = '\0';

            int record_start = offset;
            for (; record_start > 0 && raw[record_start] != '\0' && raw[record_start] != '\n'; record_start--);
            raw[record_start] = '\0';
            record_start++;

            //printf("%s\n", raw + record_start);

            sparser_records_passed++; 
            if (rapidjson_parse(raw + record_start)) {
                matching++;
            }

            offset = record_end + 1 - VECSZ;
        }

        sparser_matched += tokens_found;
    }

    double parse_time = time_stop(s);
    free(raw);

    double percent_bytes_matched = (double)(sparser_matched * strlen(search_str)) / (double)size;
    double actual_matches = ((double)matching) / ((double)sparser_records_passed); 
    double false_positives = 1.0 - actual_matches;

    // Print some statistics about what we found here.
    printf("Number of \"%s\" found by sparser: %ld (%.3f%% of the input)\n",
            search_str,
            sparser_matched,
            100.0 * (double)(sparser_matched * strlen(search_str)) / (double)size);
    printf("Fraction of sparser matches which were actual matches: %f\n", actual_matches);
    printf("Fraction of sparser matches which were false positives: %f\n", false_positives);
    printf("%ld Actual Matches\n", matching);
    printf("Runtime: %f seconds\n", parse_time);
    return parse_time;
}

// Performs a parse of the query using RapidJSON. Returns true if all the predicates match.
bool rapidjson_parse(const char *line) {
    Document d;
    d.Parse(line);
    if (d.HasParseError()) {
        fprintf(stderr, "\nError(offset %u): %s\n", 
                (unsigned)d.GetErrorOffset(),
                GetParseError_En(d.GetParseError()));
        return false;
    }

    const char *en = "en";
    const unsigned retweet_thres = 50;
    Value::ConstMemberIterator itr;

    /*
    Value::ConstMemberIterator itr = d.FindMember("lang");
    if (itr != d.MemberEnd()) {
        if (strncmp(en, itr->value.GetString(), strlen(en)) == 0) {
            return false;
        }
    } else {
        return false;
    }

    itr = d.FindMember("retweet_count");
    if (itr != d.MemberEnd()) {
        if (retweet_thres > itr->value.GetUint()) {
            return false;
        }
    } else {
        return false;
    }
    */

    itr = d.FindMember("text");
    if (itr != d.MemberEnd()) {
        if (!strstr(itr->value.GetString(), search_str)) {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

/// JSON Parser version.
double baseline_rapidjson(const char *filename) {
    char *data, *line;
    size_t bytes = read_all(filename, &data);
    int doc_index = 1;
    int matching = 0;

    bench_timer_t s = time_start();

    while ((line = strsep(&data, "\n")) != NULL) {
        if (rapidjson_parse(line)) {
            matching++;
        }
        doc_index++;
    }

    double elapsed = time_stop(s);

    printf("Passing Elements: %d of %d records (%.3f seconds)\n", matching, doc_index, elapsed);
    return elapsed;
}

int main() {
    const char *filename = path_for_data("tweets.json");
    double a = baseline(filename);
    double b = baseline_rapidjson(filename);

    printf("Speedup: %f\n", b / a);

    return 0;
}
