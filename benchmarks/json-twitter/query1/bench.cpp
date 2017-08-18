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
#include "sparser.h"

using namespace rapidjson;

#define VECSZ 32

// The query string.
const char *search_str = "Putin";
const char *search_str1 = "LaVerne";

// This is the substring to search using sparser.
const char *seek_str = "Puti";

// Callback.
bool rapidjson_parse(const char *line);

double baseline(const char *filename) {
    // Read in the data into a buffer.
    char *raw = NULL;
    long length = read_all(filename, &raw);

    bench_timer_t s = time_start();

    const char *query = "Puti";
    sparser_stats_t *stats = sparser_search_single(raw,
            length,
            query,
            4,
            rapidjson_parse);

    assert(stats);

    double parse_time = time_stop(s);

    printf("%s\n", sparser_format_stats(stats));
    printf("Runtime: %f seconds\n", parse_time);

    free(raw);
    free(stats);

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

    Value::ConstMemberIterator itr = d.FindMember("text");
    if (itr == d.MemberEnd()) {
        // The field wasn't found.
        return false;
    }
    if (strstr(itr->value.GetString(), search_str) == NULL) {
        return false;
    }

    itr = d.FindMember("user");
    if (itr == d.MemberEnd()) {
        return false;
    }

    auto user = itr->value.GetObject();
    itr = user.FindMember("name");
    if (itr == d.MemberEnd()) {
        return false;
    }
    
    if (strcmp(itr->value.GetString(), search_str1) != 0) {
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
