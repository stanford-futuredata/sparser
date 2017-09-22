#ifndef _ZAKIR_QUERIES_H_
#define _ZAKIR_QUERIES_H_

#include "json_projection.h"

// The RapidJSON query engine.
json_query_engine_t rapidjson_engine = json_query_rapidjson_execution_engine;
json_query_engine_t mison_engine = json_query_mison_execution_engine;

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

#define ZAKIR_BENCH_SPARSER

// ************************ ZAKIR QUERY 1 **************************

// Just checking for NULL.
json_passed_t zakir_q1_p23_telnet_banner_banner(const char *, void *) {
    return JSON_PASS;
}

json_passed_t zakir_q1_autonomoussystem_asn(int64_t value, void *) {
    return value == 9318 ? JSON_PASS : JSON_FAIL;
}

json_query_t zakir_query1() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "p23.telnet.banner.banner",
                                 zakir_q1_p23_telnet_banner_banner);
    json_query_add_integer_filter(query, "autonomous_system.asn",
                                  zakir_q1_autonomoussystem_asn);
    return query;
}

static const char **sparser_zakir_query1(int *count) {
    static const char *_1 = "9318";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ ZAKIR QUERY 2 **************************

json_passed_t zakir_q2_p80_http_get_body(const char *value, void *) {
    if (strstr(value, "content=\"wordpress 3.5.1")) {
        return JSON_PASS;
    }
    return JSON_FAIL;
}

json_query_t zakir_query2() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "p80.http.get.body",
                                 zakir_q2_p80_http_get_body);
    return query;
}

static const char **sparser_zakir_query2(int *count) {
    static const char *_1 = "wordpress 3.5.1";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ ZAKIR QUERY 3 **************************

json_passed_t zakir_q3_autonomoussystem_asn(int64_t value, void *) {
    return (value == 2516) ? JSON_PASS : JSON_FAIL;
}

json_query_t zakir_query3() {
    json_query_t query = json_query_new();
    json_query_add_integer_filter(query, "autonomous_system.asn",
                                  zakir_q3_autonomoussystem_asn);
    return query;
}

static const char **sparser_zakir_query3(int *count) {
    static const char *_1 = "2516";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ ZAKIR QUERY 4 **************************

json_passed_t zakir_q4_location_country(const char *value, void *) {
    return (strcmp(value, "Chile") == 0) ? JSON_PASS : JSON_FAIL;
}

// Just checking for nullity
json_passed_t zakir_q4_p80_http_get_statuscode(int64_t, void *) {
    return JSON_PASS;
}

json_query_t zakir_query4() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "location.country",
                                 zakir_q4_location_country);
    json_query_add_integer_filter(query, "p80.http.get.status_code",
                                  zakir_q4_p80_http_get_statuscode);
    return query;
}

static const char **sparser_zakir_query4(int *count) {
    static const char *_1 = "Chile";
    static const char *_2 = "status_code";
    static const char *predicates[] = {_1, _2, NULL};

    *count = 2;
    return predicates;
}

// ************************ ZAKIR QUERY 5 **************************

json_passed_t zakir_q5_p80_http_get_headers_server(const char *value, void *) {
    return strstr(value, "DIR-300") ? JSON_PASS : JSON_FAIL;
}

json_query_t zakir_query5() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "p80.http.get.headers.server",
                                 zakir_q5_p80_http_get_headers_server);
    return query;
}

static const char **sparser_zakir_query5(int *count) {
    static const char *_1 = "DIR-300";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ ZAKIR QUERY 6 **************************

// Checking for nullity
json_passed_t zakir_q6_p995_pop3s_tls_banner(const char *, void *) {
    return JSON_PASS;
}

// Checking for nullity
json_passed_t zakir_q6_p110_pop3_starttls_banner(const char *, void *) {
    return JSON_PASS;
}

json_query_t zakir_query6() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "p110.pop3.starttls.banner",
                                 zakir_q6_p110_pop3_starttls_banner);
    json_query_add_string_filter(query, "p995.pop3s.tls.banner",
                                 zakir_q6_p995_pop3s_tls_banner);
    return query;
}

static const char **sparser_zakir_query6(int *count) {
    static const char *_1 = "p110";
    static const char *_2 = "p995";
    static const char *_3 = "pop3s";
    static const char *_4 = "starttls";
    static const char *predicates[] = {_1, _2, _3, _4, NULL};

    *count = 4;
    return predicates;
}

// ************************ ZAKIR QUERY 7 **************************

// Checking for nullity
json_passed_t zakir_q7_ftp_banner_banner(const char *value, void *) {
    return strstr(value, "Seagate Central Shared") ? JSON_PASS : JSON_FAIL;
}

json_query_t zakir_query7() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "ftp.banner.banner",
                                 zakir_q7_ftp_banner_banner);
    return query;
}

static const char **sparser_zakir_query7(int *count) {
    static const char *_1 = "Seagate Central Shared";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ ZAKIR QUERY 8 **************************

// Checking for nullity
json_passed_t zakir_q8_p20000_dnp3_status_support(bool value, void *) {
    return value ? JSON_PASS : JSON_FAIL;
}

json_query_t zakir_query8() {
    json_query_t query = json_query_new();
    json_query_add_boolean_filter(query, "p20000.dnp3.status.support",
                                  zakir_q8_p20000_dnp3_status_support);
    return query;
}

static const char **sparser_zakir_query8(int *count) {
    static const char *_1 = "p20000";
    static const char *_2 = "dnp3";
    static const char *predicates[] = {_1, _2, NULL};

    *count = 2;
    return predicates;
}

// ************************ TWITTER QUERY 1 **************************
typedef struct twitter_q1_proj { long user_id; } twitter_q1_proj_t;

json_passed_t twitter_q1_text(const char *value, void *) {
    return strstr(value, "Donald Trump") ? JSON_PASS : JSON_FAIL;
}

json_passed_t twitter_q1_created_at(const char *value, void *) {
    return strstr(value, "Sep 13") ? JSON_PASS : JSON_FAIL;
}

json_passed_t twitter_q1_user_id_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    twitter_q1_proj_t *row = ((twitter_q1_proj_t *)ctx->ptr) + ctx->count;
    row->user_id = value;

    return JSON_PASS;
}

json_query_t twitter_query1() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "text", twitter_q1_text);
    // json_query_add_string_filter(query, "created_at", twitter_q1_created_at);
    json_query_add_integer_filter(query, "user.id", twitter_q1_user_id_proj);
    return query;
}

static const char **sparser_twitter_query1(int *count) {
    static const char *_1 = "Donald Trump";
    static const char *_2 = "Sep 13";
    static const char *predicates[] = {_1, _2, NULL};

    *count = 2;
    return predicates;
}

// All the queries we want to test.
const zakir_query_t queries[] = {
    zakir_query1, zakir_query2, zakir_query3, zakir_query4,   zakir_query5,
    zakir_query6, zakir_query7, zakir_query8, twitter_query1, NULL};
const sparser_zakir_query_preds_t squeries[] = {
    sparser_zakir_query1,   sparser_zakir_query2,
    sparser_zakir_query3,   sparser_zakir_query4,
    sparser_zakir_query5,   sparser_zakir_query6,
    sparser_zakir_query7,   sparser_zakir_query8,
    sparser_twitter_query1, NULL};

#endif
