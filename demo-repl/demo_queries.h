#ifndef _DEMO_QUERIES_H_
#define _DEMO_QUERIES_H_

#include "rapidjson_engine.h"

// The RapidJSON query engine.
json_query_engine_t rapidjson_engine = json_query_rapidjson_execution_engine;

typedef json_query_t (*zakir_query_t)();
typedef const char **(*sparser_zakir_query_preds_t)(int *);

// callback_info struct that is passed in as a void *
// to individual filters
typedef struct callback_info {
    unsigned long ptr;
    json_query_t query;
    long count;
    long capacity;
} callback_info_t;

// Structs for storing projected fields of queries (e.g., zakir_q9_proj_t)
// need to be packed, so that UnsafeRow reads them correctly
#pragma pack(1)

// ************************ DEMO QUERY 2 **************************

const char *DEMO_QUERY1_STR = "\n\
SELECT count(*)\n\
FROM tweets\n\
WHERE text contains \"Trump\" AND text contains \"Putin\"";

json_passed_t demo_q1_text(const char *value, void *) {
    return strstr(value, "Trump") && strstr(value, "Putin") ? JSON_PASS : JSON_FAIL;
}

json_query_t demo_query1() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "text", demo_q1_text);
    return query;
}

static const char **sparser_demo_query1(int *count) {
    static const char *_1 = "Trump";
    static const char *_2 = "Putin";
    static const char *predicates[] = {_1, _2, NULL};

    *count = 2;
    return predicates;
}

// ************** All the queries we want to test **************
const zakir_query_t demo_queries[] = {demo_query1, NULL};
const sparser_zakir_query_preds_t sdemo_queries[] = { sparser_demo_query1, NULL };
const char *demo_query_strings[] = { DEMO_QUERY1_STR, NULL };

#endif
