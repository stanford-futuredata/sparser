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

using namespace rapidjson;

// The query strings.
const char *TEXT = "Putin";
const char *USER_NAME = "LaVerne";

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

int main() {
  const char *filename = path_for_data("tweets.json");
  double a = bench_sparser(filename, TEXT, rapidjson_parse);
  double b = bench_rapidjson(filename, rapidjson_parse);

  printf("Speedup: %f\n", b / a);

  return 0;
}
