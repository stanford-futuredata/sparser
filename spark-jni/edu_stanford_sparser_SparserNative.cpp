#include "edu_stanford_sparser_SparserNative.h"
#include <jni.h>
#include <stdlib.h>
#include <iostream>
#include "bench_json.h"
#include "common.h"
#ifdef USE_HDFS
#include <hdfs/hdfs.h>
#endif
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "sparser.h"

using namespace rapidjson;

hdfsFS fs;

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
int parse_putin_russia(const char *line, void *data) {
    Document d;
    d.Parse(line);
    if (d.HasParseError()) {
        fprintf(stderr, "\nError(offset %u): %s\n",
                (unsigned)d.GetErrorOffset(),
                GetParseError_En(d.GetParseError()));
        fprintf(stderr, "Error line: %s", line);
        return false;
    }

    Value::ConstMemberIterator itr = d.FindMember("text");
    if (itr == d.MemberEnd()) {
        // The field wasn't found.
        return false;
    }
    if (strstr(itr->value.GetString(), "Putin") == NULL) {
        return false;
    }

    if (strstr(itr->value.GetString(), "Russia") == NULL) {
        return false;
    }

    // ToDo: Save projected fields instead of row indices
    return true;
}

JNIEXPORT jlong JNICALL Java_edu_stanford_sparser_SparserNative_parse(
    JNIEnv *env, jobject obj, jstring filename_java, jint filename_length_java,
    jlong buffer_addr_java, jlong start_java, jlong length_java,
    jlong record_size, jlong max_records) {
    bench_timer_t start = time_start();
    // Step 1: Convert the Java String (jstring) into C string (char*)
    char filename_c[filename_length_java];
    env->GetStringUTFRegion(filename_java, 0, filename_length_java, filename_c);
    printf("In C++, the string is: %s\n", filename_c);

    // Step 2: Benchmark Sparser (ToDo: pass in predicates as argument)
    const char *predicates[] = {
        "Putin", "Russia",
    };
    const long num_records_parsed =
        bench_sparser_spark(filename_c, start_java, length_java, predicates, 2,
                           parse_putin_russia, (void *)buffer_addr_java);
    assert(num_records_parsed <= max_records);

    const double time = time_stop(start);
    printf("Total Time in C++: %f\n", time);
    return num_records_parsed;
}

// TODO: don't hardcode HDFS hostname and port
JNIEXPORT void JNICALL
Java_edu_stanford_sparser_SparserNative_init(JNIEnv *env, jclass clazz) {
    printf("In C++, init called\n");
    // connect to NameNode
    // setenv("LIBHDFS3_CONF", "/etc/hadoop/conf/hdfs-site.xml", 1);
    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, "sparser-m");
    hdfsBuilderSetNameNodePort(builder, 8020);
    fs = hdfsBuilderConnect(builder);

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
