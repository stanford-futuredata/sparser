
#include "json_facade.h"
#include "demo_queries.h"
#include "common.h"

#include "sparser.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// This is some data we pass to the callback. In this example, we pass a JSON query
// so our JSON parser can parse the record, and a count to update the number of matching
// records.
struct callback_data {
	long count;
	json_query_t query;
};

int _rapidjson_parse_callback(const char *line, void *query) {
  if (!query) return false;
	struct callback_data *data = (struct callback_data *)query;
  char *newline = (char *)strchr(line, '\n');
  // Last one?
  if (!newline) {
    newline = (char *)(line + strlen(line) + 1);
  }
  char tmp = *newline;
  *newline = '\0';
	int passed = rapidjson_engine(data->query, line, NULL);
	if (passed) {
		data->count++;
	}
  *newline = tmp;
	return passed;
}

double bench_rapidjson_engine(char *data, long length, json_query_t query, int queryno) {
  bench_timer_t s = time_start();
  long doc_index = 1;
  long matching = 0;

  char *ptr = data;
  char *line;

	double count = 0;

  while ((line = strsep(&ptr, "\n")) != NULL) {
    if (rapidjson_engine(query, line, NULL) == JSON_PASS) {
      matching++;
    }

		count++;

		if (ptr)
			*(ptr - 1) = '\n';

    doc_index++;
  }

  double elapsed = time_stop(s);
  printf("RapidJSON:\t\t\x1b[1;33mResult: %ld (Execution Time: %f seconds)\x1b[0m\n", matching, elapsed);
  return elapsed;
}

double bench_sparser_engine(char *data, long length, json_query_t jquery, ascii_rawfilters_t *predicates, int queryno) {
  bench_timer_t s = time_start();

	struct callback_data cdata;
	cdata.count = 0;
  cdata.query = jquery;

  // XXX Generate a schedule
  sparser_query_t *query = sparser_calibrate(data, length, '\n', predicates, _rapidjson_parse_callback, &cdata);

  // XXX Apply the search.
  sparser_stats_t *stats = sparser_search(data, length, '\n', query, _rapidjson_parse_callback, &cdata);

  double elapsed = time_stop(s);
  printf("RapidJSON with Sparser:\t\x1b[1;33mResult: %ld (Execution Time: %f seconds)\x1b[0m\n", cdata.count, elapsed);

  free(stats);
  free(query);

  return elapsed;
}

#define OK       0
#define END_OF_FILE 1
#define TOO_LONG 2

// From https://stackoverflow.com/questions/4023895/how-to-read-string-entered-by-user-in-c
static int getLine (const char *prmpt, char *buff, size_t sz) {
	int ch, extra;

	// Get line with buffer overrun protection.
	if (prmpt != NULL) {
		printf ("%s", prmpt);
		fflush (stdout);
	}
	if (fgets (buff, sz, stdin) == NULL)
		return END_OF_FILE;

	// If it was too long, there'll be no newline. In that case, we flush
	// to end of line so that excess doesn't affect the next call.
	if (buff[strlen(buff)-1] != '\n') {
		extra = 0;
		while (((ch = getchar()) != '\n') && (ch != EOF))
			extra = 1;
		return (extra == 1) ? TOO_LONG : OK;
	}

	// Otherwise remove newline and give string back to caller.
	buff[strlen(buff)-1] = '\0';
	return OK;
}

void process_query(char *raw, long length, int query_index) {
	printf("Running query:\n ---------------------\x1b[1;31m%s\x1b[0m\n ---------------------\n",
      demo_query_strings[query_index]);

	int count = 0;
	json_query_t jquery = demo_queries[query_index]();
	const char ** preds = sdemo_queries[query_index](&count);

  // XXX First, get a set of candidate RFs.
	ascii_rawfilters_t d = decompose(preds, count);

	bench_sparser_engine(raw, length, jquery, &d, query_index);

	json_query_t query = demo_queries[query_index]();
	bench_rapidjson_engine(raw, length, query, query_index);

  free_ascii_rawfilters(&d);
}

void print_queries(int num_queries) {
	for (int i = 0; i < num_queries; i++) {
		printf("\x1b[1;34mQuery %d:\x1b[0m -------------\x1b[1;31m%s\x1b[0m\n ---------------------\n", i+1, demo_query_strings[i]);
	}
}

void print_usage(char **argv) {
  fprintf(stderr, "Usage: %s <JSON filename>\n", argv[0]);
}

int main(int argc, char **argv) {

  char *raw;
  long length;

	// Read in the data beforehand.
  const char *filename = argv[1];
  if(access(filename, F_OK) == -1 ) {
    print_usage(argv);
    exit(EXIT_FAILURE);
  }

	printf("Reading data...");
	fflush(stdout);
  length = read_all(filename, &raw);
	printf("done! read %f GB\n", (double)length / 1e9);
	fflush(stdout);

	// Get the number of queries.
	int num_queries = 0;
	while (demo_queries[num_queries]) {
		num_queries++;
	};

	while (true) {
    int rc;
    char buff[1024];
    rc = getLine ("Sparser> ", buff, sizeof(buff));
    if (rc == END_OF_FILE) {
        printf ("\nEOF\n");
				break;
    }

		if (rc == TOO_LONG) {
        printf ("Input too long [%s]\n", buff);
				continue;
    }

		if (strcmp(buff, "queries") == 0) {
			printf("Dataset: %s (%f GB)\n", filename, (double)length / 1e9);
			print_queries(num_queries);
			continue;
		}

		char *endptr;
		long query_index = strtoul(buff, &endptr, 10);
		if (endptr == buff || query_index > num_queries) {
			printf("Invalid query \"%s\"\n", buff);
			continue;
		}

		query_index--;
		process_query(raw, length, query_index);
	}
}
