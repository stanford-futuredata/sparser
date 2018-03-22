#ifndef _BENCH_JSON_H_
#define _BENCH_JSON_H_

#include "common.h"
#include "sparser.h"

#include <immintrin.h>

#include <assert.h>

#include "json_projection.h"

#include "gzip/gzip_uncompress.h"

typedef sparser_callback_t parser_t;

/** Uses sparser and RapidJSON to count the number of records matching the
 * search query.
 *
 * @param filename the data to check
 * @param the predicate strings.
 * @param The number of predicate strings passed.
 * @param callback the callback which invokes the full parser.
 *
 * @return number of records parsed
 */
long bench_sparser_spark(const char *filename_uri, const unsigned long start,
                         const unsigned long file_length,
                         const char **predicates, int num_predicates,
                         parser_t callback, void *callback_ctx) {
    // Read in the data into a buffer.
    char *raw = NULL;
    unsigned long length = 0;
    printf("Start: %lu\n", start);
    printf("Length: %lu\n", file_length);
    if (strncmp("hdfs://", filename_uri, 7) == 0) {
#ifdef USE_HDFS
        bench_timer_t t = time_start();
        length = read_hdfs(filename_uri, &raw, start, file_length);
        double read_hdfs_time = time_stop(t);
        printf("Read time: %f\n", read_hdfs_time);
        assert(length == file_length + 1);
#endif
    } else if (strncmp("file:///", filename_uri, 8) == 0) {
        bench_timer_t t = time_start();
        length = read_local(filename_uri, &raw, start, file_length);
        double read_local_time = time_stop(t);
        printf("Read time: %f\n", read_local_time);
    }

    bench_timer_t s = time_start();
    sparser_query_t *query =
        sparser_calibrate(raw, length, predicates, num_predicates, callback);
    assert(query);
    double parse_time = time_stop(s);

    printf("Calibration Runtime: %f seconds\n", parse_time);

    s = time_start();
    sparser_stats_t *stats =
        sparser_search(raw, length, query, callback, callback_ctx);
    assert(stats);
    parse_time += time_stop(s);

    printf("%s\n", sparser_format_stats(stats));
    printf("Total Runtime: %f seconds\n", parse_time);

    free(query);
    const long num_records_passed = stats->callback_passed;
    free(stats);
    free(raw);

    return num_records_passed;
}

/** Uses sparser and RapidJSON to count the number of records matching the
 * search query.
 *
 * @return the running time.
 */
double bench_sparser(const char *filename, int is_compressed,
                     const char **predicates, int num_predicates,
                     parser_t callback, void *context) {
    double total_time = 0;

    // Read in the data into a buffer.
    bench_timer_t t = time_start();

    char *compressed = NULL;
    char *raw = NULL;
    long compressed_length = read_all(filename, &compressed);

    double read_time = time_stop(t);
    printf("Read time: %f\n", read_time);

    total_time += read_time;

    long length;
    if (is_compressed) {
        t = time_start();
        length = uncompress_gzip(compressed, &raw, compressed_length - 1);
        // eh
        raw[length] = 0;
        printf("Data length: %ld\n", length);
        double uncompress_time = time_stop(t);
        free(compressed);
        printf("Decompression time: %f\n", uncompress_time);
        total_time += uncompress_time;
    } else {
        length = compressed_length;
        raw = compressed;
        printf("Decompression time: 0.0 (uncompressed data)\n");
    }

    t = time_start();
    sparser_query_t *query =
        sparser_calibrate(raw, length, predicates, num_predicates, callback);
    assert(query);
    double calibrate_time = time_stop(t);
    total_time += calibrate_time;

    printf("Calibration Runtime: %f seconds\n", calibrate_time);

    t = time_start();
    sparser_stats_t *stats =
        sparser_search(raw, length, query, callback, context);
    assert(stats);
    double search_time = time_stop(t);
    total_time += search_time;
    printf("Search time: %f seconds\n", search_time);

    printf("%s\n", sparser_format_stats(stats));
    printf("SPARSER Total Runtime: %f seconds\n", total_time);

    free(query);
    free(stats);
    free(raw);

    return total_time;
}

/* Times splitting the input by newline and calling the full parser on each
 * line.
 *
 * @param filename
 * @param callback the function which performs the parse.
 *
 * @return the running time.
 */
double bench_rapidjson(const char *filename, parser_t callback,
                       void *callback_ctx) {
    char *data, *line;
    read_all(filename, &data);
    int doc_index = 1;
    int matching = 0;

    bench_timer_t s = time_start();

    char *ptr = data;
    while ((line = strsep(&ptr, "\n")) != NULL) {
        if (callback(line, callback_ctx)) {
            matching++;
        }
        doc_index++;
    }

    double elapsed = time_stop(s);
    printf("JSON PARSER Passing Elements: %d of %d records (%.3f seconds)\n",
           matching, doc_index, elapsed);

    free(data);

    return elapsed;
}

double bench_rapidjson_compressed(const char *filename, int is_compressed,
                                  parser_t callback, void *callback_ctx) {
    char *data, *compressed, *line;
    double total_time = 0;

    bench_timer_t s = time_start();

    size_t raw_length = read_all(filename, &compressed);
    int doc_index = 1;
    int matching = 0;

    double read_time = time_stop(s);
    printf("Read time %f\n", read_time);
    total_time += read_time;

    if (is_compressed) {
        s = time_start();
        uncompress_gzip(compressed, &data, raw_length - 1);
        double uncompress_time = time_stop(s);
        total_time += uncompress_time;
        printf("Decompression time: %f\n", uncompress_time);
        free(compressed);
    } else {
        data = compressed;
        printf("Decompression time: 0.0 (Uncompressed data)\n");
    }

    s = time_start();
    char *ptr = data;
    while ((line = strsep(&ptr, "\n")) != NULL) {
        if (callback(line, callback_ctx)) {
            matching++;
        }
        doc_index++;
    }

    double parse_time = time_stop(s);
    total_time += parse_time;
    printf("Parse time: %f\n", parse_time);
    printf("JSON PARSER Passing Elements: %d of %d records (%.3f seconds)\n",
           matching, doc_index, total_time);

    free(data);

    return total_time;
}

/* Times splitting the input by newline and calling the full parser on each
 * line.
 *
 * @param filename
 * @param callback the function which performs the parse.
 *
 * @return the running time.
 */
double bench_json_with_api(const char *filename, json_query_t query,
                           json_query_engine_t engine, void *udata) {
    char *data, *line;
    read_all(filename, &data);
    int doc_index = 1;
    int matching = 0;

    bench_timer_t s = time_start();

    char *ptr = data;
    while ((line = strsep(&ptr, "\n")) != NULL) {
        if (engine(query, line, udata) == JSON_PASS) {
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
