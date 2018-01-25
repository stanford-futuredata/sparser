
#include "json_projection.h"
#include "queries.h"
#include "common.h"

#include "sparser.h"

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

double bench_mison_engine(char *data, long length, json_query_t query, int queryno) {

  bench_timer_t s = time_start();

  long doc_index = 1;
  long matching = 0;

  char *ptr = data;
  char *line;
  while ((line = strsep(&ptr, "\n")) != NULL) {
    if (mison_engine(query, line, NULL) == JSON_PASS) {
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

int _mison_parse_callback(const char *line, void * _) {
  size_t length = strlen(line);
  if (length == 0) {
    return false;
  }
  intptr_t x = mison_parse(line, length);
  return (x == 0);
}

double bench_sparser_engine(char *data, long length, json_query_t jquery, const char **preds, int num_preds, int queryno) {

  bench_timer_t s = time_start();

  long doc_index = 1;
  long matching = 0;

  sparser_query_t *query = sparser_calibrate(data, length, preds, num_preds, _mison_parse_callback);
  sparser_stats_t *stats = sparser_search(data, length, query, _mison_parse_callback, NULL);

  double elapsed = time_stop(s);
  printf("Passing Elements: %ld of %ld records\n",
      matching,
      doc_index);
  printf("Query %d Execution Time: %f seconds\n", queryno, elapsed);

  assert(stats);
  printf("%s\n", sparser_format_stats(stats));
  free(stats);
  free(query);

  return elapsed;
}

double bench_strstr_rapidjson_engine(char *data,
    long length, json_query_t jquery,
    const char **preds,
    int num_preds,
    int queryno) {

  bench_timer_t s = time_start();

  long doc_index = 1;
  long matching = 0;

  char *ptr = data;
  char *line;
  while ((line = strsep(&ptr, "\n")) != NULL) {
    int passed = 1;

    // Check for everything.
    for (int i = 0; i < num_preds; i++) {
      if (strstr(line, preds[i]) == NULL) {
        passed = 0;
        break;
      }
    }


    if (passed && rapidjson_engine(jquery, line, NULL) == JSON_PASS) {
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
  //const char *filename = "/lfs/1/sparser/zakir-small.json";
  length = read_all(filename, &raw);

  int query_index;

#ifdef ZAKIR_BENCH_RJ
  printf("----------------> Benchmarking RapidJSON\n");
  query_index = 0;
  while (queries[query_index]) {
    json_query_t query = queries[query_index]();
    printf("Running Query %d\n", query_index);
    bench_rapidjson_engine(raw, length, query, query_index + 1);
    query_index++;
  }
#endif

#ifdef ZAKIR_BENCH_MISON
  printf("----------------> Benchmarking Mison\n");
  query_index = 0;
  while (queries[query_index]) {
    json_query_t query = queries[query_index]();
    printf("Running Query %d\n", query_index);
    bench_mison_engine(raw, length, query, query_index + 1);
    query_index++;
  }
#endif

#ifdef ZAKIR_BENCH_SPARSER
  printf("----------------> Benchmarking Sparser + RapidJSON\n");
  query_index = 0;
  while (squeries[query_index]) {
    int count;
    json_query_t jquery = queries[query_index]();
    const char ** preds = squeries[query_index](&count);
    printf("Running Query %d\n", query_index);

    bench_sparser_engine(raw, length, jquery, preds, count, query_index + 1);
    query_index++;
  }
#endif

#ifdef ZAKIR_BENCH_STRSTR
  printf("----------------> Benchmarking strstr + RapidJSON\n");
  query_index = 0;
  while (squeries[query_index]) {
    int count;
    json_query_t jquery = queries[query_index]();
    const char ** preds = squeries[query_index](&count);
    printf("Running Query %d\n", query_index);

    bench_strstr_rapidjson_engine(raw, length, jquery, preds, count, query_index + 1);
    query_index++;
  }
#endif
}
