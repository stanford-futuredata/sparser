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
#include "sparser.h"

using namespace rapidjson;

// The query strings.
const char *TEXT = "Putin";
const char *TEXT2 = "Russia";

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
bool rapidjson_parse(const char *line) {
  Document d;
  d.Parse(line);
  if (d.HasParseError()) {
    fprintf(stderr, "\nError(offset %u): %s\n", (unsigned)d.GetErrorOffset(),
            GetParseError_En(d.GetParseError()));
    fprintf(stderr, "Error line: %s", line);
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

  return true;
}

int main() {
  const char *filename = path_for_data("tweets.json");

  sparser_query_t squery;
  memset(&squery, 0, sizeof(squery));
  // assert(sparser_add_query(&squery, "Russ") == 0);
  assert(sparser_add_query(&squery, "Puti") == 0);

  double a = bench_sparser(filename, &squery, rapidjson_parse);
  double b = bench_rapidjson(filename, rapidjson_parse);

  bench_read(filename);

  printf("Speedup: %f\n", b / a);

  return 0;
}
