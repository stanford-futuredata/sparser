
#include <time.h>

#include "json_projection.h"
#include "queries.h"
#include "decompose.h"
#include "common.h"

#include "sparser.h"

// Common: 30722
// Rare:  3320

// Modify this to vary the selectivity of the ASN.
#define ASN_TO_SEARCH 	   "30722"
#define ASN_TO_SEARCH_INT   30722

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
                                 zakir_q1_p23_telnet_banner_banner_mod);
    json_query_add_integer_filter(query, "autonomous_system.asn",
                                  zakir_q1_autonomoussystem_asn_mod);
    return query;
}

static const int *sparser_zakir_query1_source(int *count) {
    static const int _1 = 0;
    static const int _2 = 1;
    static const int _3 = 1;
    static const int _4 = 1;
    static const int _5 = 1;
    static const int _6 = 2;
    static const int _7 = 2;
    static const int _8 = 2;
    static const int _9 = 2;
    static const int _10 = 3;
    static const int _11 = 3;
    static const int _12 = 3;
    static const int _13 = 3;
    static const int _14 = 3;
    static const int _15 = 3;
    static const int _16 = 3;
    static const int _17 = 3;
    static const int _18 = 4;
    static const int _19 = 5;
    static const int predicates[] = {_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19};

    *count = 19;
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

  sparser_add_query(query, ASN_TO_SEARCH);
  sparser_add_query(query, "teln");

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


#define MAX_LENGTH  3

void run_sparser(char *data, long length, json_query_t jquery, const char **schedule, const int *sched_source, int num_preds) {

	char *buf = (char *)calloc(1, 2048);
	for (int j = 0; j < num_preds; j++) {
		strcat(buf, schedule[j]);
		strcat(buf, " ");
	}

	parse_time = 0;
	bench_timer_t s = time_start();

	for (int i = 0; i < num_preds; i++) {
		for (int j = 0; j < num_preds; j++) {
			if (i == j) continue;
			if (sched_source[i] == sched_source[j]) {
				fprintf(stderr, "Skipping schedule %s due to overlap.\n", buf);
				free(buf);
				return;
			}
		}
	}

	fprintf(stderr, "Handling schedule %s\n", buf);

	sparser_query_t *query = (sparser_query_t *)calloc(1, sizeof(sparser_query_t));

	for (int i = 0; i < num_preds; i++) {
		sparser_add_query(query, schedule[i]);
	}

	sparser_stats_t *stats = sparser_search(data, length, query, _rapidjson_parse_callback, &jquery);

	double elapsed = time_stop(s);

	printf("%s: %f seconds\n", buf, elapsed);
	fflush(stdout);

	free(buf);
	assert(stats);
	fprintf(stderr, "%s\n", sparser_format_stats(stats));
	free(stats);
	free(query);
}

/** Generates all combinations of up to length `len`. */
void process_combinations(
	const char **preds, const int preds_len, const int *preds_source,
	int len, int start,
	const char **result, const int result_len, int *result_source,
	char *data, long data_length, json_query_t jquery) {

	if (len == 0) {
		run_sparser(data, data_length, jquery, result, result_source, result_len);
		return;
	}

	for (int i = start; i <= preds_len - len; i++) {
		result[result_len - len] = preds[i];
		result_source[result_len - len] = preds_source[i];
		process_combinations(preds, preds_len, preds_source,
			len - 1, i + 1,
			result, result_len, result_source,
			data, data_length, jquery);
	}
}

void bench_sparser_engine_all_preds(char *data,
		long data_length,
		json_query_t jquery,
		const char **preds,
		const int *pred_source,
		int num_preds) {

	const char *schedule[MAX_LENGTH];
	int schedule_source[MAX_LENGTH];

	for (int sched_length = 1; sched_length <= MAX_LENGTH; sched_length++) {
		process_combinations(preds, num_preds, pred_source,
				sched_length, 0,
				schedule, sched_length, schedule_source,
				data, data_length, jquery);
	}
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
  sparser_add_query(query, "p23");
  sparser_add_query(query, "teln");
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

void bench_sparser_engine_new(char *data, long length, json_query_t jquery, decomposed_t *d) {
	sparser_query_t *query = sparser_calibrate(data, length, '\n', d, _rapidjson_parse_callback);
  sparser_stats_t *stats = sparser_search(data, length, query, _rapidjson_parse_callback, &jquery);

  assert(stats);
  printf("%s\n", sparser_format_stats(stats));
  free(stats);
  free(query);
}

int main(int argc, char **argv) {

  char *raw;
  long length;

  //const char *filename = "/lfs/1/sparser/zakir14g.json";
  const char *filename = "/lfs/1/sparser/zakir-small.json";
  length = read_all(filename, &raw);

// For the non-independence experiment.
#if 0
  fprintf(stderr, "----------------> Benchmarking Sparser\n");
  int count = 0;
  json_query_t jquery = zakir_query1_mod();
  const char ** preds = sparser_zakir_query1_mod(&count);
  fprintf(stderr, "Running Zakir Query\n");

  bench_sparser_engine(raw, length, jquery, preds, count);

  // Hard code the schedule here based on knowledge of how common each predicate is.
  fprintf(stderr, "----------------> Benchmarking Naive Sched.\n");
  bench_sparser_engine_naive(raw, length, jquery);
#else

  // For the "exhaustive schedule  search" experiment.
  int count = 0;
  json_query_t jquery = zakir_query1_mod();
  const char **preds = sparser_zakir_query1(&count);

	decomposed_t d = decompose(preds, count);

  //fprintf(stderr, "----------------> Benchmarking All Schedules.\n");
  //bench_sparser_engine_all_preds(raw, length, jquery, d.strings, d.sources, d.num_strings);
  bench_sparser_engine_new(raw, length, jquery, &d);
#endif

}
