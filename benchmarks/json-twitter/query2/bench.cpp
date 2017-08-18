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

const char *CREATED_AT = "2017";
const char *ENTITIES_HASHTAG_TEXT = "maga";

// For printing debug information.
static char print_buffer[4096];

// Callback
bool rapidjson_parse(const char *line);

// Applies sparser + RapidJSON for a full parse if sparser returns a positive signal.
double baseline(const char *filename, const char *query) {
  // Read in the data into a buffer.
  char *raw = NULL;
  long length = read_all(filename, &raw);

  bench_timer_t s = time_start();

  int query_length;
  if (!query) {
    query = ENTITIES_HASHTAG_TEXT;
    query_length = 4;
  } else {
    query_length = strlen(query);
    if (query_length > 4) {
      query_length = 4;
    }
  }

  sparser_stats_t *stats =
      sparser_search_single(raw, length, query, 4, rapidjson_parse);

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

    Value::ConstMemberIterator itr = d.FindMember("created_at");
    if (itr == d.MemberEnd()) {
        // The field wasn't found.
        return false;
    }
    if (strstr(itr->value.GetString(), CREATED_AT) == NULL) {
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
        if (strcmp(itr2->value.GetString(), ENTITIES_HASHTAG_TEXT) == 0) {
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
    const char *filename = path_for_data("tweets.json");
    double a = baseline(filename, NULL);
    double b = baseline_rapidjson(filename);

    printf("Speedup: %f\n", b / a);

    return 0;
}
