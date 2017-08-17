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

// This is the substring to search using sparser.
const char *seek_str = "2017";

// For printing debug information.
static char print_buffer[4096];

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
    long sparser_matched = 0;
    // Number of records sparser chose to pass to the full parser.
    long sparser_records_passed = 0;
    // Number of records which actually matched using the full parser.
    long matching = 0;

    // This is the string to search.
    // TODO - generalize this - the check function should be a function pointer set according to the number of bytes
    // we want to match.
    //
    // Currently set to 32-bit matching (so we should use .._epi32() everywhere).
    uint32_t search = *((uint32_t *)seek_str);
    __m256i xs = _mm256_set1_epi32(search);

    RESET_PRINTER();
    _mm256_storeu_si256((__m256i *)print_buffer, xs);
    printf("%s\n", print_buffer);

    bool swapped = false;
    for (size_t offset = 0; offset < size - VECSZ - 3; offset += VECSZ) {
        int tokens_found = 0;

        // Swap half way through from 2017 -> maga
        if (!swapped && offset >= size / 1000) {
            swapped = true;
            printf("swapping\n");
            const char *seek_str2 = "maga";
            uint32_t search = *((uint32_t *)seek_str2);
            xs = _mm256_set1_epi32(search);
        }

        // Fuzzy check
        tokens_found += check(xs, raw + offset);
        tokens_found += check(xs, raw + offset + 1);
        tokens_found += check(xs, raw + offset + 2);
        tokens_found += check(xs, raw + offset + 3);

        // Fuzzy check passed - use full parser to verify.
        if (tokens_found) {
            // Race forward to null-terminate so we can pass the token to the parser.
            long record_end = offset;
            long checked_neighborhood = false;
            for (; record_end < size && raw[record_end] != '\n'; record_end++) {

                // neighborhood search.
                /*
                if (!checked_neighborhood && raw[record_end] == ':') {
                    int searcher = record_end - VECSZ;
                    __m256i val = _mm256_loadu_si256((__m256i const *)(raw + searcher));
                    __m256i checker = _mm256_set1_epi8('x');
                    unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(checker, val));
                    if (!mask) {
                        goto next;
                    }
                    checked_neighborhood = true;
                }
                */

            }
            raw[record_end] = '\0';

            // Seek back to the previous newline so we can pass the full record to the parser.
            long record_start = offset;
            for (; record_start > 0 && raw[record_start] != '\0' && raw[record_start] != '\n'; record_start--);
            if (record_start != 0) {
                raw[record_start] = '\0';
                record_start++;
            }

            sparser_records_passed++; 
            if (rapidjson_parse(raw + record_start)) {
                matching++;
            }
            offset = record_end + 1 - VECSZ;
        }

        sparser_matched += tokens_found;

next:
        {}
    }

    double parse_time = time_stop(s);
    free(raw);

    double percent_bytes_matched = (double)(sparser_matched * strlen(seek_str)) / (double)size;
    double actual_matches;
    double false_positives;
    if (sparser_records_passed > 0) {
        actual_matches = ((double)matching) / ((double)sparser_records_passed); 
        false_positives = 1.0 - actual_matches;
    } else {
        actual_matches = 0;
        false_positives = 0;
    }

    // Print some statistics about what we found here.
    printf("Number of \"%s\" found by sparser: %ld (%.3f%% of the input)\n",
            seek_str,
            sparser_matched,
            100.0 * (double)(sparser_matched * strlen(seek_str)) / (double)size);
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

    Value::ConstMemberIterator itr = d.FindMember("created_at");
    if (itr == d.MemberEnd()) {
        // The field wasn't found.
        return false;
    }
    if (strstr(itr->value.GetString(), "2017") == NULL) {
        return false;
    }

    itr = d.FindMember("entities");
    if (itr == d.MemberEnd()) {
        return false;
    }

    auto entities = itr->value.GetObject();
    itr = entities.FindMember("hashtags");
    if (itr == d.MemberEnd()) {
        return false;
    }

    for (auto& v : itr->value.GetArray()) {
        Value::ConstMemberIterator itr2 = v.GetObject().FindMember("text");
        if (itr2 == v.MemberEnd()) {
            // The field wasn't found.
            return false;
        }

        // Found it!
        if (strcmp(itr2->value.GetString(), "maga") == 0) {
            return true;
        }
    }

    return false;
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
    const char *filename = path_for_data("tweets-large.json");
    double a = baseline(filename);
    double b = baseline_rapidjson(filename);

    printf("Speedup: %f\n", b / a);

    return 0;
}
