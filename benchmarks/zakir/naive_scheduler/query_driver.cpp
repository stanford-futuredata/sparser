
#include <time.h>

#include "json_projection.h"
#include "queries.h"
#include "common.h"

#include "sparser.h"

// Modify this to vary the selectivity of the ASN.
#define ASN_TO_SEARCH "9318"
#define ASN_TO_SEARCH_INT 9318

/*
 * Benchmarks the sparser scheduler (sparser_calibrate) against a "naive" scheduler
 * which does not consider the non-independence among different queries.
 *
 * This experiment requires some setup:
 *
 * 1. Modify the ASN to be either common or uncommon.
 * 2. Obtain the ground truth probabilities of the individual filters, (E.g., with grep or something)
 * and then use the min-probability filters to generate the naive schedule.
 *
 */

// ************************ ZAKIR QUERY 1 MODIFIED  **************************
/**
 * SELECT COUNT(*)
 * FROM  ipv4.20160425
 * WHERE p23.telnet.banner.banner is not NULL
 * AND   autonomous_system.asn = CONFIGURABLE;
 **/

// Just checking for NULL.
json_passed_t zakir_q1_p23_telnet_banner_banner_mod(const char *, void *) {
    return JSON_PASS;
}

json_passed_t zakir_q1_autonomoussystem_asn_mod(int64_t value, void *) {
    return value == ASN_TO_SEARCH_INT ? JSON_PASS : JSON_FAIL;
}

json_query_t zakir_query1_mod() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "p23.telnet.banner.banner",
                                 zakir_q1_p23_telnet_banner_banner);
    json_query_add_integer_filter(query, "autonomous_system.asn",
                                  zakir_q1_autonomoussystem_asn);
    return query;
}

static const char **sparser_zakir_query1_mod(int *count) {
    static const char *_1 = ASN_TO_SEARCH;
    static const char *_2 = "telnet";
    static const char *_3 = "banner";
    static const char *_4 = "autonomous_system";
    static const char *_5 = "asn";
    static const char *_6 = "p23";
    static const char *predicates[] = {_1, _2, _3, _4, _5, _6, NULL};

    *count = 6;
    return predicates;
}

//////////////////////////////////////////////////////////////////////

int _mison_parse_callback(const char *line, void * _) {
  size_t length = strlen(line);
  if (length == 0) {
    return false;
  }
  intptr_t x = mison_parse(line, length);
  return (x != 0);
}

static double parse_time = 0;

int _rapidjson_parse_callback(const char *line, void *query) {
  if (!query) return false;

  bench_timer_t s = time_start();
	int passed = rapidjson_engine(*((json_query_t *)query), line, NULL);
	double elapsed = time_stop(s);

	parse_time += elapsed;
	return passed;
}

double bench_sparser_engine(char *data,
    long length,
    json_query_t jquery,
    const char **preds,
    int num_preds) {

  parse_time = 0;
  bench_timer_t s = time_start();

  sparser_query_t *query = (sparser_query_t *)calloc(1, sizeof(sparser_query_t));

  sparser_add_query(query, "teln");
  sparser_add_query(query, "9318");

  //sparser_query_t *query = sparser_calibrate(data, length, preds, num_preds, _mison_parse_callback);
  sparser_stats_t *stats = sparser_search(data, length, query, _rapidjson_parse_callback, &jquery);

  double elapsed = time_stop(s);
  printf("Query Execution Time: %f seconds\n", elapsed);
	printf("Parsing Time: %f seconds\n", parse_time);

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
    json_query_t jquery) {

  parse_time = 0;
  bench_timer_t s = time_start();

  sparser_query_t *query = (sparser_query_t *)calloc(1, sizeof(sparser_query_t));

  // We just grep for these terms offline to get this schedule...
  sparser_add_query(query, "teln");
  sparser_add_query(query, "bann");
  sparser_stats_t *stats = sparser_search(data, length, query, _rapidjson_parse_callback, &jquery);

  double elapsed = time_stop(s);

  printf("Query Execution Time: %f seconds\n", elapsed);
	printf("Parsing Time: %f seconds\n", parse_time);

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

  printf("----------------> Benchmarking Sparser\n");
  int count = 0;
  json_query_t jquery = zakir_query1_mod();
  const char ** preds = sparser_zakir_query1_mod(&count);
  printf("Running Zakir Query\n");

  bench_sparser_engine(raw, length, jquery, preds, count);

  // Hard code the schedule here based on knowledge of how common each predicate is.
  printf("----------------> Benchmarking Naive Sched.\n");
  bench_sparser_engine_naive(raw, length, jquery);
}
