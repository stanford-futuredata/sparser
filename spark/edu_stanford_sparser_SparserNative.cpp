#include "edu_stanford_sparser_SparserNative.h"
#include <jni.h>
#include <stdlib.h>
#include <iostream>
##include <hdfs/hdfs.h>
#include "common.h"
#include "sparser.h"

using namespace rapidjson;

hdfsFS fs;

#include "json_facade.h"

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
        bench_timer_t t = time_start();
        length = read_hdfs(filename_uri, &raw, start, file_length);
        double read_hdfs_time = time_stop(t);
        printf("Read time: %f\n", read_hdfs_time);
        assert(length == file_length + 1);
    } else if (strncmp("file:///", filename_uri, 8) == 0) {
        // bench_timer_t t = time_start();
        length = read_local(filename_uri, &raw, start, file_length);
        double read_local_time = time_stop(t);
        printf("Read time: %f\n", read_local_time);
    }

    // bench_timer_t s = time_start();
    sparser_query_t *query =
        sparser_calibrate(raw, length, predicates, num_predicates, callback);
    assert(query);
    double parse_time = time_stop(s);

    printf("Calibration Runtime: %f seconds\n", parse_time);

    // s = time_start();
    sparser_stats_t *stats =
        sparser_search(raw, length, query, callback, callback_ctx);
    assert(stats);
    // parse_time += time_stop(s);

    printf("%s\n", sparser_format_stats(stats));
    printf("Total Runtime: %f seconds\n", parse_time);

    free(query);
    const long num_records_passed = stats->callback_passed;
    free(stats);
    free(raw);

    return num_records_passed;
}

// Performs a full parse of the query using RapidJSON
int full_parser_callback(const char *line, void *thunk) {
    callback_info_t *info = (callback_info_t *)thunk;
    json_query_t query = info->query;

    if (!thunk) return 1;

    const int ret = rapidjson_engine(query, line, thunk);
    if (ret)
        // IMPT: increment count, so we know which row to
        // write to for the projections
        info->count++;
    return ret;
}

JNIEXPORT jlong JNICALL Java_edu_stanford_sparser_SparserNative_parse(
    JNIEnv *env, jobject, jstring filename_java, jint filename_length,
    jlong buffer_addr, jlong start, jlong length, jint query_index,
    jlong max_records) {
    bench_timer_t start_time = time_start();
    // Convert the Java String (jstring) into C string (char*)
    char filename_c[filename_length];
    env->GetStringUTFRegion(filename_java, 0, filename_length, filename_c);
    printf("In C++, the string is: %s\n", filename_c);

    // Benchmark Sparser
    int count;
    json_query_t query = queries[query_index]();
    const char **preds = squeries[query_index](&count);

    callback_info_t ctx;
    ctx.query = query;
    ctx.count = 0;
    ctx.capacity = max_records;
    ctx.ptr = buffer_addr;

    const long num_records_parsed = bench_sparser_spark(
        filename_c, start, length, preds, count, full_parser_callback, &ctx);

    assert(num_records_parsed <= max_records);

    const double time = time_stop(start_time);
    printf("Total Time in C++: %f\n", time);
    return num_records_parsed;
}

// TODO: don't hardcode HDFS hostname and port
JNIEXPORT void JNICALL Java_edu_stanford_sparser_SparserNative_init(JNIEnv *,
                                                                    jclass) {
    printf("In C++, init called\n");
#ifdef USE_HDFS
    // connect to NameNode
    setenv("LIBHDFS3_CONF", "/etc/hadoop/conf/hdfs-site.xml", 1);
    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, "sparser-m");
    hdfsBuilderSetNameNodePort(builder, 8020);
    fs = hdfsBuilderConnect(builder);
#endif

    // Code for finding hostname; use this later when hostname is no longer
    // hard-coded

    // char *filename = (char *)filename_uri + 7;
    // while (*filename != '/') {
    //     ++filename;
    // }
    // const unsigned int hostname_length = filename - (filename_uri + 7);
    // char *hostname = (char *)malloc(hostname_length + 1);
    // strncpy(hostname, filename_uri + 7, hostname_length);
    // hostname[hostname_length] = '\0';

    // TODO: also add a teardown method, so we have the place
    // to call the following code
    // hdfsFreeBuilder(builder);
    // free(hostname);
}
