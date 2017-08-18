#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <string.h>

#include <arpa/inet.h>
#include <immintrin.h>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "common.h"
#include "sparser.h"

using namespace rapidjson;

// The query string.
const char *TEXT = "Putin";
const char *USER_NAME = "LaVerne";

// Callback - parses using RapidJSON and returns true if passed *all*
// predicates.
bool rapidjson_parse(const char *line);

/** Uses sparser and RapidJSON to count the number of records matching the
 * predicate.
 *
 * @param filename the data to check
 * @param query a search query. If this is NULL, TEXT is used. At most four
 * bytes from this query will be used.
 *
 * @return the running time.
 */
double baseline(const char *filename, const char *query) {
  // Read in the data into a buffer.
  char *raw = NULL;
  long length = read_all(filename, &raw);

  bench_timer_t s = time_start();

  int query_length;
  if (!query) {
    query = TEXT;
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

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
bool rapidjson_parse(const char *line) {
  Document d;
  d.Parse(line);
  if (d.HasParseError()) {
    fprintf(stderr, "\nError(offset %u): %s\n", (unsigned)d.GetErrorOffset(),
            GetParseError_En(d.GetParseError()));
    return false;
  }

  Value::ConstMemberIterator itr = d.FindMember("text");
  if (itr == d.MemberEnd()) {
    // The field wasn't found.
    return false;
  }
  if (strstr(itr->value.GetString(), TEXT) == NULL) {
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

  if (strcmp(itr->value.GetString(), USER_NAME) != 0) {
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
  printf("Passing Elements: %d of %d records (%.3f seconds)\n", matching,
         doc_index, elapsed);
  return elapsed;
}

int main() {
  const char *filename = path_for_data("tweets.json");
  double a = baseline(filename, TEXT);
  double b = baseline_rapidjson(filename);

  printf("Speedup: %f\n", b / a);

  return 0;
}
