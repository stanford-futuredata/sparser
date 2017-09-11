#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <string.h>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include <bench_json.h>

#include <json_projection.h>

using namespace rapidjson;

const char *CREATED_AT = "2017";
const char *ENTITIES_HASHTAG_TEXT = "Trump";

int test_projection(const char *line) {
  json_query_t query = json_query_new();
  json_query_add_projection(query, "user.followers_count", JSON_TYPE_INT);
  json_query_add_projection(query, "user.id", JSON_TYPE_INT);
  json_query_add_projection(query, "user.name", JSON_TYPE_STRING);
  json_query_add_string_filter(query, "user.name", "smith");

  // Currently just prints stuff and returns NULL
  json_query_rapidjson_execution_engine(query, line);
  printf("\n");


  // For debugging.
  //json_query_print(query);
  
  return 0;
}


// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
int rapidjson_parse(const char *line) {
  Document d;
  d.Parse(line);
  if (d.HasParseError()) {
    fprintf(stderr, "\nError(offset %u): %s\n", (unsigned)d.GetErrorOffset(),
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

  for (auto &v : itr->value.GetArray()) {
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

int main() {
  const char *filename = path_for_data("tweets-small.json");
  //double b = bench_rapidjson(filename, rapidjson_parse);
  double a = bench_rapidjson(filename, test_projection);

  return 0;
}
