#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <string.h>

#include <immintrin.h>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "common.h"

using namespace rapidjson;

#define VECSZ 32

// The query string.
const char *search_str = "StartCom Certification Authority";
const char *search_str1 = "France";

static const char *kTypeNames[] = {"Null",  "False",  "True",  "Object",
                                   "Array", "String", "Number"};

// This is the substring to search using sparser.
const char *seek_str = "artC";

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
    // Set to 0x11111111 for epi32, 0xf0f0f0f0f0 for epi16. No mask for
    // byte-level comparator.
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

// Applies sparser + RapidJSON for a full parse if sparser returns a positive
// signal.
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
    // Total number of documents.
    long doc_index = 1;

    // This is the string to search.
    // TODO - generalize this - the check function should be a function pointer
    // set according to the number of bytes
    // we want to match.
    //
    // Currently set to 32-bit matching (so we should use .._epi32()
    // everywhere).
    uint32_t search = *((uint32_t *)seek_str);
    __m256i xs = _mm256_set1_epi32(search);

    RESET_PRINTER();
    _mm256_storeu_si256((__m256i *)print_buffer, xs);
    printf("%s\n", print_buffer);

    for (size_t offset = 0; offset < size - VECSZ - 3; offset += VECSZ) {
        int tokens_found = 0;

        // Fuzzy check
        tokens_found += check(xs, raw + offset);
        tokens_found += check(xs, raw + offset + 1);
        tokens_found += check(xs, raw + offset + 2);
        tokens_found += check(xs, raw + offset + 3);

        // Fuzzy check passed - use full parser to verify.
        if (tokens_found) {
            // Race forward to null-terminate so we can pass the token to the
            // parser.
            long record_end = offset;
            for (; record_end < size && raw[record_end] != '\n'; record_end++)
                ;
            raw[record_end] = '\0';

            // Seek back to the previous newline so we can pass the full record
            // to the parser.
            long record_start = offset;
            for (; record_start > 0 && raw[record_start] != '\0' &&
                   raw[record_start] != '\n';
                 record_start--)
                ;
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
    }

    double parse_time = time_stop(s);
    free(raw);

    // double percent_bytes_matched = (double)(sparser_matched *
    // strlen(search_str)) / (double)size;
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
    printf(
        "Number of \"%s\" found by sparser: %ld (%.3f%% of the input)\n",
        search_str, sparser_matched,
        100.0 * (double)(sparser_matched * strlen(search_str)) / (double)size);
    printf("Fraction of sparser matches which were actual matches: %f\n",
           actual_matches);
    printf("Fraction of sparser matches which were false positives: %f\n",
           false_positives);
    printf("%ld Actual Matches\n", matching);
    printf("Runtime: %f seconds\n", parse_time);
    return parse_time;
}

// return p7547.cwmp.get.body != NULL
bool parse_cwmp(Document &d) {
    Value::ConstMemberIterator itr = d.FindMember("p7547");
    if (itr == d.MemberEnd()) {
        return false;
    }

    auto p7547 = itr->value.GetObject();
    itr = p7547.FindMember("cwmp");
    auto cwmp = itr->value.GetObject();

    itr = cwmp.FindMember("get");
    if (itr == d.MemberEnd()) {
        return false;
    }

    auto get = itr->value.GetObject();
    itr = get.FindMember("body");
    return (itr != d.MemberEnd());
}

// .p443.https.tls.chain[0].parsed.issuer.common_name[0]
bool parse_certificate(Document &d, const char *search_str) {
    Value::ConstMemberIterator itr = d.FindMember("p443");
    if (itr == d.MemberEnd()) {
        // The field wasn't found.
        return false;
    }

    auto p443 = itr->value.GetObject();
    itr = p443.FindMember("https");
    // don't need to check "https" always follows "p443"
    auto https = itr->value.GetObject();

    itr = https.FindMember("tls");
    if (itr == d.MemberEnd()) {
        return false;
    }

    // StringBuffer sb;
    // PrettyWriter<StringBuffer> writer(sb);
    // itr->value.Accept(writer);
    // auto str = sb.GetString();
    // printf("Value: %s\n", str);

    auto tls = itr->value.GetObject();
    // TODO: remove this, figure out why
    // Find Member doesn't work
    if (!tls.HasMember("chain")) {
        return false;
    }

    itr = tls.FindMember("chain");
    /*
    if (itr == d.MemberEnd()) {
        return false;
    }
    */

    auto chain = itr->value.GetArray();
    auto first_cert = chain[0].GetObject();

    itr = first_cert.FindMember("parsed");
    if (itr == d.MemberEnd()) {
        return false;
    }

    auto parsed = itr->value.GetObject();
    itr = parsed.FindMember("issuer");
    if (itr == d.MemberEnd()) {
        return false;
    }

    auto issuer = itr->value.GetObject();
    // TODO: remove this, too
    if (!issuer.HasMember("common_name")) {
        return false;
    }

    itr = issuer.FindMember("common_name");
    auto common_name = itr->value.GetArray();

    return strcmp(common_name[0].GetString(), search_str) == 0;
}

bool parse_country(Document &d, const char *search_str) {
    Value::ConstMemberIterator itr = d.FindMember("location");
    if (itr == d.MemberEnd()) {
        // The field wasn't found.
        return false;
    }

    auto location = itr->value.GetObject();
    itr = location.FindMember("country");
    if (itr == d.MemberEnd()) {
        return false;
    }

    return strcmp(itr->value.GetString(), search_str) == 0;
}

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
bool rapidjson_parse(const char *line) {
    Document d;
    d.Parse(line);
    // printf("%s\n", line);
    if (d.HasParseError()) {
        fprintf(stderr, "\nError(offset %u): %s\n",
                (unsigned)d.GetErrorOffset(),
                GetParseError_En(d.GetParseError()));
        return false;
    }

    return parse_certificate(d, search_str) && parse_country(d, search_str1);
}

/// JSON Parser version.
double baseline_rapidjson(const char *filename) {
    char *data, *line;
    size_t bytes = read_all(filename, &data);
    int doc_index = 0;
    int matching = 0;

    bench_timer_t s = time_start();

    while (strlen(line = strsep(&data, "\n")) != 0) {
        if (rapidjson_parse(line)) {
            matching++;
        }
        doc_index++;
    }

    double elapsed = time_stop(s);

    printf("Passing Elements: %d of %d records (%.3f seconds)\n", matching,
           doc_index, elapsed);
    return elapsed;
}

int main() {
    const char *filename =
        path_for_data("zakir/04-01-2017_filtered_1m_samples.json");
    // const char *filename = path_for_data("zakir/test.json");
    double a = baseline(filename);
    double b = baseline_rapidjson(filename);

    printf("Speedup: %f\n", b / a);

    return 0;
}
