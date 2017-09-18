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

#include "bench_json.h"
#include "json_projection.h"
#include "sparser.h"

using namespace rapidjson;

// The query strings.
const char *TEXT = "Putin";
const char *TEXT2 = "Russia";

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
int rapidjson_parse(const char *line, void *thunk) {
  Document d;
  d.Parse(line);
  if (d.HasParseError()) {
    fprintf(stderr, "\nError(offset %u): %s\n", (unsigned)d.GetErrorOffset(),
            GetParseError_En(d.GetParseError()));
    //fprintf(stderr, "Error line: %s", line);
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

  if (strstr(itr->value.GetString(), TEXT2) == NULL) {
    return false;
  }

  itr = d.FindMember("id");
  if (itr == d.MemberEnd()) {
    // The field wasn't found.
    return false;
  }
  return true;
}


// Callback.
json_passed_t check_string(const char *value, void *data) {
  if (strstr(value, TEXT) == NULL || strstr(value, TEXT2) == NULL) {
    return JSON_FAIL;
  }
  return JSON_PASS;
}

int main() {
  const char *filename = path_for_data("tweets-trump.json");

  double a = bench_rapidjson(filename, rapidjson_parse, NULL);

  json_query_t query = json_query_new();
  json_query_add_string_filter(query, "text", check_string);

  double b = bench_json_with_api(filename, query, json_query_rapidjson_execution_engine, NULL);

  printf("Speedup (should be close to 1.0): %f\n", b/a);

  return 0;
}
