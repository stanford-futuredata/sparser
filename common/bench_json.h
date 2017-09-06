#ifndef _QUERY_SPARSER_H_
#define _QUERY_SPARSER_H_

#include "common.h"
#include "sparser.h"

#include <immintrin.h>

#include <assert.h>

typedef sparser_callback_t parser_t;

/** Uses sparser and RapidJSON to count the number of records matching the
 * search query.
 *
 * @param filename the data to check
 * @param the predicate strings.
 * @param The number of predicate strings passed.
 * @param callback the callback which invokes the full parser.
 *
 * @return the running time.
 */
double bench_sparser(const char *filename, const char **predicates,
                      int num_predicates, parser_t callback) {

  // Read in the data into a buffer.
  char *raw = NULL;
  long length = read_all(filename, &raw);

  bench_timer_t s = time_start();
  sparser_query_t *query = sparser_calibrate(raw, length, predicates, num_predicates, callback);
  assert(query);
  double parse_time = time_stop(s);

  printf("Calibration Runtime: %f seconds\n", parse_time);

  s = time_start();
  sparser_stats_t *stats = sparser_search(raw, length, query, callback);
  assert(stats);
  parse_time += time_stop(s);

  printf("%s\n", sparser_format_stats(stats));
  printf("Total Runtime: %f seconds\n", parse_time);

  free(query);
  free(stats);
  free(raw);

  return parse_time;
}

/* Times splitting the input by newline and calling the full parser on each
 * line.
 *
 * @param filename
 * @param callback the function which performs the parse.
 *
 * @return the running time.
 */
double bench_rapidjson(const char *filename, parser_t callback) {
  char *data, *line;
  read_all(filename, &data);
  int doc_index = 1;
  int matching = 0;

  bench_timer_t s = time_start();

  char *ptr = data;
  while ((line = strsep(&ptr, "\n")) != NULL) {
    if (callback(line)) {
      matching++;
    }
    doc_index++;
  }

  double elapsed = time_stop(s);
  printf("Passing Elements: %d of %d records (%.3f seconds)\n", matching,
         doc_index, elapsed);

  free(ptr);

  return elapsed;
}

/* Times splitting the input by newline and calling the full parser on each
 * line. Uses the Mison Leveled Colon Bitmap technique, but doesn't implement
 * pattern trees - this can be seen as a lower bound on parse time.
 *
 * @param filename
 * @param callback the function which performs the parse.
 *
 * @return the running time.
 */
double bench_mison(const char *filename, parser_t callback) {
  char *data, *line;
  read_all(filename, &data);
  int doc_index = 1;
  int matching = 0;

  double elapsed = 0;

  char *ptr = data;
  while ((line = strsep(&ptr, "\n")) != NULL) {
    bench_timer_t s = time_start();
    if (callback(line)) {
      matching++;
    }
    elapsed += time_stop(s);
    doc_index++;
  }

  printf("Passing Elements: %d of %d records (%.3f seconds)\n", matching,
         doc_index, elapsed);

  free(data);

  return elapsed;
}

double bench_read(const char *filename) {
  char *data;
  long bytes = read_all(filename, &data);

  bench_timer_t s = time_start();

  __m256i sum = _mm256_setzero_si256();
  for (long i = 0; i < bytes; i += 32) {
    __m256i x = _mm256_loadu_si256((__m256i *)(data + i));
    sum = _mm256_add_epi32(x, sum);
  }

  double elapsed = time_stop(s);

  int out[32];
  _mm256_storeu_si256((__m256i *)out, sum);
  for (int i = 1; i < 32; i++) {
    out[0] += out[i];
  }

  printf("Read Benchmark Result: %d (%f seconds)\n", out[0], elapsed);
  free(data);
  return elapsed;
}

#endif
