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
#include "mison.h"

using namespace rapidjson;

// The query strings.
const char *TEXT = "Putin";
const char *TEXT2 = "Donald Trump";

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
int rapidjson_parse(const char *line, void * _) {
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

  return true;
}

int mison_parse_wrapper(const char *line, void * _) {
  size_t length = strlen(line);
  if (length == 0) {
    return false;
  }

  intptr_t x = mison_parse(line, length);
  return (x == 0);
}

int main() {
  const char *filename = path_for_data("tweets.json");

  char *predicates[] = { NULL, NULL, NULL, };
  char *first, *second;
  asprintf(&first, "%s", TEXT);
  asprintf(&second, "%s", TEXT2);

  predicates[0] = first;
  predicates[1] = second;

  double a = bench_sparser(filename, (const char **)predicates, 2, mison_parse_wrapper);

  //double b = bench_rapidjson(filename, rapidjson_parse);

  // bench_rapidjson actually works for any generic parser.
  double b = bench_rapidjson(filename, mison_parse_wrapper);

  free(first);
  free(second);

  bench_read(filename);

  printf("Speedup: %f\n", b / a);

  return 0;
}
