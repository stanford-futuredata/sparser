
#include "json_projection.h"
#include "queries.h"
#include "common.h"

#include "sparser.h"

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

// Runs the first Zakir query with "ground truth" knowledge of the distribution of each
// predicate, choosing the top two rarest ones for search.
double bench_sparser_engine_naive(char *data,
    long length,
    json_query_t jquery,
    int queryno) {

  bench_timer_t s = time_start();

  long doc_index = 1;
  long matching = 0;

  sparser_query_t *query = (sparser_query_t *)calloc(1, sizeof(sparser_query_t));
  sparser_add_query(query, "teln");
  sparser_add_query(query, "bann");
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


// Index of the first query.
#define FIRST_QUERY 0

int main(int argc, char **argv) {

  char *raw;
  long length;

  const char *filename = "/lfs/1/sparser/zakir-small.json";
  length = read_all(filename, &raw);

  printf("----------------> Benchmarking Sparser + RapidJSON\n");
  int count = 0;
  json_query_t jquery = queries[FIRST_QUERY]();
  const char ** preds = squeries[FIRST_QUERY](&count);
  printf("Running Query %d\n", FIRST_QUERY);

  bench_sparser_engine(raw, length, jquery, preds, count, FIRST_QUERY + 1);

  // Hard code the schedule here based on knowledge of how common each predicate is.
  bench_sparser_engine_naive(raw, length, jquery, FIRST_QUERY + 1);
}
