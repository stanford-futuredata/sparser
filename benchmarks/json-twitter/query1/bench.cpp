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
const char *TEXT2 = "Russia";

// Data passed to parser callback in this query.
struct parser_data {
  // Number of ids recorded so far.
  long count;
  // Amount of space allocated in IDs
  long capacity;
  // Buffer for writing the data. @Firas, in Spark, this should be set to the pointer
  // of unsafe rows.
  int64_t *ids;
};

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

  // Must handle this!!
  if (!thunk) return true;

  // XXX The assumption here is that thunk has allocated enough memory
  // for each ID!!!
  struct parser_data *pd = (struct parser_data *)thunk;

  // We messed up -- just abort.
  assert (pd->count < pd->capacity); 

  pd->ids[pd->count] = itr->value.GetInt64();
  pd->count++;
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
  const char *filename = path_for_data("tweets-trump.json");

  char *predicates[] = { NULL, NULL, NULL, };
  char *first, *second;
  asprintf(&first, "%s", TEXT);
  asprintf(&second, "%s", TEXT2);

  predicates[0] = first;
  predicates[1] = second;

  struct parser_data pd;
  memset(&pd, 0, sizeof(pd));
  pd.capacity = 100000;
  pd.ids = (int64_t *)malloc(sizeof(int64_t) * pd.capacity);

  double a = bench_sparser(filename, (const char **)predicates, 2, rapidjson_parse, &pd);

  printf("IDs from sparser\n");
  for (int i = 0; i < pd.count; i++) {
    //printf("%lld\n", pd.ids[i]);
  }
  pd.count = 0;

  //double b = bench_rapidjson(filename, rapidjson_parse);

  // bench_rapidjson actually works for any generic parser.
  double b = bench_rapidjson(filename, rapidjson_parse, &pd);

  printf("IDs from RapidJSON\n");
  for (int i = 0; i < pd.count; i++) {
    //printf("%lld\n", pd.ids[i]);
  }

  free(first);
  free(second);

  free(pd.ids);

  bench_read(filename);

  printf("Speedup: %f\n", b / a);

  return 0;
}
