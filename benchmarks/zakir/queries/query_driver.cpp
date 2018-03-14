
#include "json_projection.h"
#include "queries.h"
#include "common.h"

#include "sparser.h"

int _rapidjson_parse_callback(const char *line, void *query) {
  if (!query) return false;
	int passed = rapidjson_engine(*((json_query_t *)query), line, NULL);
	return passed;
}

int _mison_parse_callback(const char *line, void * _) {
  size_t length = strlen(line);
  if (length == 0) {
    return false;
  }
  intptr_t x = mison_parse(line, length);
  return (x == 0);
}

double bench_rapidjson_engine(char *data, long length, json_query_t query, int queryno) {
  bench_timer_t s = time_start();
  long doc_index = 1;
  long matching = 0;

  char *ptr = data;
  char *line;

	double total = 0;
	double count = 0;

  while ((line = strsep(&ptr, "\n")) != NULL) {

		double start = rdtsc();
    if (rapidjson_engine(query, line, NULL) == JSON_PASS) {
      matching++;
    }
		double end = rdtsc();

		total += (end - start);
		count++;

		if (ptr)
			*(ptr - 1) = '\n';

		if (count == 10) break;

    doc_index++;
  }

	printf("Average cycles/parse: %f\n", total / count);

  double elapsed = time_stop(s);
  fprintf(stderr, "Passing Elements: %ld of %ld records\n",
      matching,
      doc_index);
  fprintf(stderr, "Query %d Execution Time: %f seconds\n", queryno, elapsed);
  return elapsed;
}

double bench_mison_engine(char *data, long length, json_query_t query, int queryno) {
  bench_timer_t s = time_start();
  long doc_index = 1;
  long matching = 0;

  char *ptr = data;
  char *line;

	double total = 0;
	double count = 0;

  while ((line = strsep(&ptr, "\n")) != NULL) {

		double start = rdtsc();
    if (mison_engine(query, line, NULL) == JSON_PASS) {
      matching++;
    }
		double end = rdtsc();
		total += (end - start);
		count++;

		if (ptr)
			*(ptr - 1) = '\n';
			
		if (count == 10) break;

    doc_index++;
  }

	printf("Average cycles/parse: %f\n", total / count);

  double elapsed = time_stop(s);
  printf("Passing Elements: %ld of %ld records\n",
      matching,
      doc_index);
  printf("Query %d Execution Time: %f seconds\n", queryno, elapsed);
  return elapsed;
}

double bench_scan_engine(char *data, long length, json_query_t query, int queryno) {
  bench_timer_t s = time_start();
  long doc_index = 1;
  long matching = 0;

  char *ptr = data;
  char *line;

	double total = 0;
	double count = 0;

  while ((line = strsep(&ptr, "\n")) != NULL) {

		size_t result;
		double start = rdtsc();
    if ((result = strlen(line)) == JSON_PASS) {
      matching++;
    }
		double end = rdtsc();
		total += (end - start);
		count++;

		printf("%zu\n", result);

		if (ptr)
			*(ptr - 1) = '\n';
			
    doc_index++;
  }

	printf("Average cycles/parse: %f\n", total / count);

  double elapsed = time_stop(s);
  printf("Passing Elements: %ld of %ld records\n",
      matching,
      doc_index);
  printf("Query %d Execution Time: %f seconds\n", queryno, elapsed);
  return elapsed;
}

double bench_sparser_engine(char *data, long length, json_query_t jquery, decomposed_t *predicates, int queryno) {
  bench_timer_t s = time_start();
  long doc_index = 1;
  long matching = 0;

  sparser_query_t *query = sparser_calibrate(data, length, '\n', predicates, _rapidjson_parse_callback, &jquery);
  sparser_stats_t *stats = sparser_search(data, length, query, _rapidjson_parse_callback, &jquery);

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

int main(int argc, char **argv) {

  char *raw;
  long length;

  const char *filename = "/lfs/1/sparser/zakir14g.json";
  //const char *filename = "/lfs/1/sparser/zakir-small.json";
  length = read_all(filename, &raw);

  int query_index;

#if 1
  printf("----------------> Benchmarking RapidJSON\n");
  query_index = 0;
  while (queries[query_index]) {
    json_query_t query = queries[query_index]();
    fprintf(stderr, "Running Query %d\n", query_index);
    bench_rapidjson_engine(raw, length, query, query_index + 1);
    query_index++;
  }
#endif

#if 1
  printf("----------------> Benchmarking Mison\n");
  query_index = 0;
  while (queries[query_index]) {
    json_query_t query = queries[query_index]();
    fprintf(stderr, "Running Query %d\n", query_index);
    bench_mison_engine(raw, length, query, query_index + 1);
    query_index++;
  }
#endif

#if 0
  printf("----------------> Benchmarking Scan\n");
  query_index = 0;
  while (queries[query_index]) {
    json_query_t query = queries[query_index]();
    fprintf(stderr, "Running Query %d\n", query_index);
    bench_scan_engine(raw, length, query, query_index + 1);
    query_index++;
  }
#endif

#if 1
  printf("----------------> Benchmarking Sparser + RapidJSON\n");
  query_index = 0;
  while (squeries[query_index]) {
    int count;
    json_query_t jquery = queries[query_index]();
    const char ** preds = squeries[query_index](&count);
    printf("Running Query %d\n", query_index);

		// TODO free this!
		decomposed_t d = decompose(preds, count);
    bench_sparser_engine(raw, length, jquery, &d, query_index + 1);
    query_index++;
  }
#endif
}
