/**
 *
 * sparser.h
 * Version 0.1.0
 *
 * Implements the sparser optimizer and the main search functionality.
 *
 * NOTE: This version of sparser is simplified and relies on the two-way search algorithm
 * in glibc's memmem/strstr implementation rather than on explicit x86-based vector instructions.
 * Glibc's memmem is highly optimized for small strings, so Sparser's optimizer still selects
 * an effective filter cascade by measuring passthrough rates.
 *
 * This version of the schedule currently only considers conjunctive queries, but extending it to
 * disjunctions is relatively straightforward and will be implemented in a subsequent release.
 *
 * Change Log:
 *
 * V0.1.0
 *
 * Initial Public release
 *
 */
#ifndef _SPARSER_H_
#define _SPARSER_H_

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <string.h>

#include <arpa/inet.h>

// TODO(shoumik): This should be an interface different formats implement:
// right now its tied to ASCII-based formats.
#include "decompose_ascii_rawfilters.h"
#include "bitmap.h"
#include "rdtsc.h"

// For debugging
#ifdef DEBUG
#define SPARSER_DBG(...) do{ fprintf( stderr, __VA_ARGS__ ); } while( false )
#else
#define SPARSER_DBG(...) do{ } while ( false )
#endif

// Checks if bit i is set in n.
#define IS_SET(n, i) (n & (0x1L << i))

// Max size of a single search string.
const int SPARSER_MAX_QUERY_LENGTH = 16;
// Max number of search strings in a single query.
const int SPARSER_MAX_QUERY_COUNT = 32;

// Max substrings to consider.
const int MAX_SUBSTRINGS = 32;
// Max records to sample.
const int MAX_SAMPLES  = 1000;
// Max record depth.
const int MAX_SCHEDULE_SIZE  = 4;

const int PARSER_MEASUREMENT_SAMPLES = 10;

typedef char BYTE;

// Defines a sparser query, which is currently a set of conjunctive string
// terms that we search for.
typedef struct sparser_query_ {
    unsigned count;
    char queries[SPARSER_MAX_QUERY_COUNT][SPARSER_MAX_QUERY_LENGTH];
    size_t lens[SPARSER_MAX_QUERY_COUNT];
} sparser_query_t;

// The callback for the calibrate and search functions. The callback MUST be
// able to handle NULL data (i.e., it should check for NULL and  still return
// true or false appropriately).
//
// The callback takes a pointer to a set of bytes and the context object that
// was passed into `sparser_search` and `sparser_calibrate`. The context object
// can be used to "process" the data (e.g., count the number of passing records,
// store which pointers Sparser passed, perform projections, etc.).
//
// The `input` pointer passed via the callback is guarnateed to be in the data buffer
// passed to `sparser_search` and `sparser_calibrate`. The callback should not modify
// the length of the data buffer, as this can cause errors when the callback returns.
//
// XXX For now, a "record" starts at the specified delimiter character in
// `sparser_search`.  For binary data, this doesn't work since there usually
// isn't a single delimiting character between records. We still need to add
// support for this.
//
// In the calibration function, the callback should return non-zero if the
// search predicate passed the record passed to the callback and 0 otherwise.
// In the search function, the callback *should* return non-zero if the search
// predciate passes, but this is only used for recalibration and is not
// strictly necessary. Users can pass different callback functions to calibrate
// and search, and can also pass different callbacks across calls to search if
// the same `sparser_query_t` is used on different batches of data.
typedef int (*sparser_callback_t)(const BYTE *input, void *);

typedef struct sparser_stats_ {
		// Number of records processed.
		long records;
    // Number of times the search query matched.
    long total_matches;
    // Number of records sparser passed.
    long sparser_passed;
    // Number of records the callback passed by returning true.
    long callback_passed;
    // Total number of bytes we had to walk forward to see a new record,
    // when a match was found.
    long bytes_seeked_forward;
    // Total number of bytes we had to walk backward to see a new record,
    // when a match was found.
    long bytes_seeked_backward;
    // Fraction that sparser passed that the callback also passed
    double fraction_passed_correct;
    // Fraction of false positives.
    double fraction_passed_incorrect;
} sparser_stats_t;

typedef struct search_data {
	// Number of records sampled.
	double num_records;	
	// The false positive masks for each sample.
	bitmap_t *passthrough_masks;
	// Cost of the full parser.
	double full_parse_cost;
	// Best cost so far.
	double best_cost;
	// Best schedule (indexes into ascii_rawfilters_t).
	int best_schedule[MAX_SCHEDULE_SIZE];
	// Length of the full parser.
	int schedule_len;

	// The joint bitmap (to prevent small repeated malloc's)
	bitmap_t joint;

	// number of schedules skipped.
	long skipped;
	// number of schedules processed.
	long processed;
	// Total cycles spent *processing and skipping*.
	long total_cycles;

} search_data_t;

/* Return a new empty sparser query. */
sparser_query_t *sparser_new_query() {
    return (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
}

/* Adds a search term to the query.
 *
 * @param query the query
 * @param string the search string.
 *
 * @return 0 if successful, nonzero otherwise.
 */
int sparser_add_query(sparser_query_t *query, const void *string, size_t len) {
    if (query->count >= SPARSER_MAX_QUERY_COUNT) {
        return -1;
    }

    // Clamp to the max length.
    len = SPARSER_MAX_QUERY_LENGTH < len ? SPARSER_MAX_QUERY_LENGTH : len;
    memcpy(query->queries[query->count], string, len);
    query->lens[query->count] = len;
    query->count++;

    return 0;
}

/** Cost in CPU cycles of a raw filter which searches for a term of length `len`. */
double rf_cost(const size_t len) {
	return len * 8.0;
}

/** Searches through combinations of raw filters to evaluate the best schedule. 
 * 
 * @param predicates the predicates to search through
 * @param len the number of predicates
 * @param start the start offset, used to generate combinations
 * @param result the indices of the chosen cascades.
 * @param result_len the number of chosen cascades (and the length of result)
 * 
 */
void search_schedules(ascii_rawfilters_t *predicates,
		search_data_t *sd,
		int len,
		int start,
		int *result,
		const int result_len) {

#ifdef DEBUG
	static char printer[4096];
#endif

	// Base case - compute the cost.
	if (len == 0) {

		long start = rdtsc();

		assert(result_len > 0);

#ifdef DEBUG
		printer[0] = 0;
		for (int i = 0; i < result_len; i++) {
			strcat(printer, predicates->strings[result[i]]);	
			strcat(printer, " ");
		}
		SPARSER_DBG("Considering schedule %s...", printer);
#endif

		for (int i = 0; i < result_len; i++) {
			for (int j = 0; j < result_len; j++) {
        // Skip records with the same "source" string (e.g., don't consider both 'Athe' and 'hena' from Athena)
				if (i != j && predicates->sources[result[i]] == predicates->sources[result[j]]) {
					SPARSER_DBG("\x1b[0;33mskipped\x1b[0m due to duplicate source!\n");
					long end = rdtsc();
					sd->skipped++;
					sd->total_cycles += (end - start);
					return;
				}
			}
		}

		SPARSER_DBG("\x1b[0;32mprocessing!\x1b[0m\n");

		int first_index = result[0];
		bitmap_t *joint = &sd->joint;
		bitmap_copy(joint, &sd->passthrough_masks[first_index]);

		// First filter runs unconditionally.
		double total_cost = rf_cost(predicates->lens[first_index]);

		for (int i = 1; i < result_len; i++) {
			int index = result[i];
			uint64_t joint_rate = bitmap_count(joint);
			double filter_cost = rf_cost(predicates->lens[index]);
			double rate = ((double)joint_rate) / sd->num_records;
			SPARSER_DBG("\t Rate after %s: %f\n", predicates->strings[result[i-1]], rate);
			total_cost += filter_cost * rate;

			bitmap_and(joint, joint, &sd->passthrough_masks[index]);	
		}

		// Account for full parser.
		uint64_t joint_rate = bitmap_count(joint);
		double filter_cost = sd->full_parse_cost;
		double rate = ((double)joint_rate) / sd->num_records;
		SPARSER_DBG("\t Rate after %s (rate of full parse): %f\n", predicates->strings[result[result_len-1]], rate);
		total_cost += filter_cost * rate;
		SPARSER_DBG("\tCost: %f\n", total_cost);

		if (total_cost < sd->best_cost) {
			assert(result_len <= MAX_SCHEDULE_SIZE);
			memcpy(sd->best_schedule, result, sizeof(int) * result_len);
			sd->schedule_len = result_len;
		}

		long end = rdtsc();
		sd->processed++;
		sd->total_cycles += end - start;
		return;
	}

	// Recursively find schedules.
	for (int i = start; i <= predicates->num_strings - len; i++) {
		// Just record the index into predicates.
		result[result_len - len] = i;
		search_schedules(predicates, sd, len - 1, i + 1, result, result_len);
	}
}

struct calibrate_timing {
	double sampling_total;
	double searching_total;
	double grepping_total;

	long cycles_per_schedule_avg;
	long cycles_per_parse_avg;

	// scheudles.
	long processed;
	long skipped;

	double total;
};

void print_timing(struct calibrate_timing *t) {
	SPARSER_DBG("Calibrate Sampling Total: %f\n\
Calibrate Searching Total: %f\n\
Calibrate Grepping Total: %f\n\
Cycles/Schedule: %lu\n\
%% Schedules Skipped: %f\n\
Cycles/Parse: %lu\n\
Total Time: %f\n",
	t->sampling_total,
	t->searching_total,
	t->grepping_total,
	t->cycles_per_schedule_avg,
	((double)t->skipped) / ((double)(t->processed + t->skipped)) * 100.0,
	t->cycles_per_parse_avg,
	t->total);
}

/** Returns a search query given a sample input and a set of predicates. The
 * returned search query
 * attempts to jointly minimize the search time and false positive rate.
 *
 * @param sample the sample to test.
 * @param length the length of the sample.
 * @param predicates a set of full predicates.
 * @param count the number of predicates to test.
 * @param callback the callback, which specifies whether a query passes.
 *
 * @return a search query, or NULL if an error occurred. The returned query
 * should be returned with free().
 */
sparser_query_t *sparser_calibrate(BYTE *sample,
		long length,
		BYTE delimiter,
		ascii_rawfilters_t *predicates,
		sparser_callback_t callback,
		void *callback_arg) {

		struct calibrate_timing timing;
		memset(&timing, 0, sizeof(timing));
		bench_timer_t start_e2e = time_start();

    // Stores false positive mask for each predicate.
    // Bit `i` is set if the ith false positive record was *passed* by the
    // predicate.
    bitmap_t passthrough_masks[MAX_SUBSTRINGS];
    for (int i = 0; i < MAX_SUBSTRINGS; i++) {
        passthrough_masks[i] = bitmap_new(MAX_SAMPLES);
    }

		// The number of substrings to process.
		int num_substrings = predicates->num_strings > MAX_SUBSTRINGS ? MAX_SUBSTRINGS : predicates->num_strings;

    // Counts number of records processed thus far.
    long records = 0;
		long parsed_records = 0;
		long passed = 0;
    unsigned long parse_cost = 0;

		bench_timer_t start = time_start();

    // Now search for each substring in up to MAX_SAMPLES records.
    char *line, *newline;
    size_t remaining_length = length;
    while (records < MAX_SAMPLES &&
           (newline = (char *)memchr(sample, delimiter, remaining_length)) != NULL) {

        // Emulates behavior of strsep, but uses memchr's faster implementation.
        line = sample;
        sample = newline + 1;
        remaining_length -= (sample - line);

				bench_timer_t grep_timer = time_start();
        for (int i = 0; i < num_substrings; i++) {
            const char *predicate = predicates->strings[i];
						SPARSER_DBG("grepping for %s...", predicate);

            if (memmem(line, newline - line, predicate, predicates->lens[i])) {
                // Set this record to found for this substring.
                bitmap_set(&passthrough_masks[i], records);
								SPARSER_DBG("found!\n");
            } else {
							SPARSER_DBG("not found.\n");
						}
        }
				double grep_time = time_stop(grep_timer);
				timing.grepping_total += grep_time;

				// To estimate the full parser's cost.
				if (records < PARSER_MEASUREMENT_SAMPLES) {
					unsigned long start = rdtsc();
					passed += callback(line, callback_arg);
					unsigned long end = rdtsc();
					parse_cost += (end - start);
					parsed_records++;
				}

        records++;

				timing.cycles_per_parse_avg = parse_cost;
    }

		timing.sampling_total = time_stop(start);
		start = time_start();

		SPARSER_DBG("%lu passed\n", passed);

		// The average parse cost.
		parse_cost = parse_cost / parsed_records;

		search_data_t sd;
		memset(&sd, 0, sizeof(sd));
		sd.num_records = records;
		sd.passthrough_masks = passthrough_masks;
		sd.full_parse_cost = parse_cost;
		sd.best_cost = 0xffffffff;
		sd.joint = bitmap_new(MAX_SAMPLES);

		// temp buffer to store the result.
		int result[MAX_SCHEDULE_SIZE];

		// Get the best schedule.
		for (int i = 1; i <= MAX_SCHEDULE_SIZE; i++) {
				search_schedules(predicates, &sd, i, 0, result, i);
		}

		timing.searching_total = time_stop(start);
		timing.cycles_per_schedule_avg = sd.total_cycles / sd.processed;

		timing.processed = sd.processed;
		timing.skipped = sd.skipped;

		static char printer[4096];
		printer[0] = 0;
		for (int i = 0; i < sd.schedule_len; i++) {
			strcat(printer, predicates->strings[sd.best_schedule[i]]);	
			strcat(printer, " ");
		}
		SPARSER_DBG("Best schedule: %s\n", printer);

    sparser_query_t *squery = sparser_new_query();
    memset(squery, 0, sizeof(sparser_query_t));
		for (int i = 0; i < sd.schedule_len; i++) {
			sparser_add_query(squery, predicates->strings[sd.best_schedule[i]],
          predicates->lens[sd.best_schedule[i]]);
		}

    for (int i = 0; i < MAX_SUBSTRINGS; i++) {
        bitmap_free(&passthrough_masks[i]);
    }

		timing.total = time_stop(start_e2e);
		print_timing(&timing);

		bitmap_free(&sd.joint);

    return squery;
}

/* Performs the sparser search given a compiled search query and a buffer.
 *
 * This performs a simple sparser search given the query and input buffer. It
 * only searches for one occurance of the query string in each record. Records
 * are assumed to be delimited by newline.
 *
 * @param input the buffer to search
 * @param length the size of the input buffer, in bytes.
 * @param query the query to look for
 * @param callback the callback if sparser passes the query.
 *
 * @return statistics about the run.
 * */
sparser_stats_t *sparser_search(char *input, long length, BYTE delimiter,
                                sparser_query_t *query,
                                sparser_callback_t callback,
                                void *callback_ctx) {

		for (unsigned i = 0; i < query->count; i++) {
			 SPARSER_DBG("Search string %d: %s\n", i+1, query->queries[i]);
		}

    sparser_stats_t stats;
    memset(&stats, 0, sizeof(stats));

		// Last byte in the data.
		char *input_last_byte = input + length - 1;

		// Points to the end of the current record.
		char *current_record_end;
		// Points to the start of the current record.
		char *current_record_start;

		current_record_start = input;

		while (current_record_start < input_last_byte) {

			stats.records++;

      // TODO for binary data, we don't want to do this: Instead, we search forward
      // until we find a match, and then scan forward in the callback until the match point.
			current_record_end = (char *)memchr(current_record_start, delimiter,
          input_last_byte - current_record_start);
			if (!current_record_end) {
				current_record_end = input_last_byte;
			}

      size_t record_length = current_record_end - current_record_start;
			unsigned count = 0;
			// Search for each of the raw filters.
			for (unsigned i = 0; i < query->count; i++) {
				if (memmem(current_record_start, record_length, query->queries[i], query->lens[i]) == NULL) {
					break;	
				}

				stats.total_matches++;
				count++;
			}

			// If all raw filters matched...
			if (count == query->count) {
				stats.sparser_passed++;

				if (callback(current_record_start, callback_ctx)) {
					stats.callback_passed++;
				}

			}

			// Update to point to the next record. The top of the loop will update the remaining variables.
			current_record_start = current_record_end + 1;
		}

    if (stats.sparser_passed > 0) {
        stats.fraction_passed_correct =
            (double)stats.callback_passed / (double)stats.sparser_passed;
        stats.fraction_passed_incorrect = 1.0 - stats.fraction_passed_correct;
    }

    sparser_stats_t *ret = (sparser_stats_t *)malloc(sizeof(sparser_stats_t));
    memcpy(ret, &stats, sizeof(stats));

    return ret;
}

static char *sparser_format_stats(sparser_stats_t *stats) {
    static char buf[8192];

    snprintf(buf, sizeof(buf),
		"Records Processed: %ld\n\
Distinct Query matches: %ld\n\
Sparser Passed Records: %ld\n\
Callback Passed Records: %ld\n\
Bytes Seeked Forward: %ld\n\
Bytes Seeked Backward: %ld\n\
Fraction Passed Correctly: %f\n\
Fraction False Positives: %f",
						 stats->records,
             stats->total_matches, stats->sparser_passed,
             stats->callback_passed, stats->bytes_seeked_forward,
             stats->bytes_seeked_backward, stats->fraction_passed_correct,
             stats->fraction_passed_incorrect);
    return buf;
}

#endif
