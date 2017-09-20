
#include "json_projection.h"
#include "zakir_queries.h"
#include "common.h"

typedef json_query_t (*zakir_query_t)();

// All the queries we want to test.
const zakir_query_t queries[] = {
  zakir_query1,
  zakir_query2,
  zakir_query3,
  zakir_query4,
  zakir_query5,
  zakir_query6,
  zakir_query7,
  zakir_query8,
  NULL
};

double bench_rapidjson_engine(char *data, long length, json_query_t query, int queryno) {

  bench_timer_t s = time_start();

  long doc_index = 1;
  long matching = 0;

  char *ptr = data;
  char *line;
  while ((line = strsep(&ptr, "\n")) != NULL) {
    if (rapidjson_engine(query, line, NULL) == JSON_PASS) {
      matching++;
    }
	
		if (ptr)
			*(ptr - 1) = '\n';

    doc_index++;
  }

  double elapsed = time_stop(s);
  printf("Passing Elements: %ld of %ld records\n",
      matching,
      doc_index);
  printf("Query %d Execution Time: %f seconds\n", queryno, elapsed);
  return elapsed;
}

int main(int argc, char **argv) {

  char *raw;
  long length;

  const char *filename = "/lfs/1/sparser/04-25-2016-truncated.json";
  length = read_all(filename, &raw);

  int query_index = 0;
  while (queries[query_index]) {
    json_query_t query = queries[query_index]();
    printf("Running Query %d\n", query_index);
    bench_rapidjson_engine(raw, length, query, query_index + 1);
    query_index++;
  } 
}
