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
/**
 * SELECT COUNT(*)
 * FROM  ipv4.20160425
 * WHERE p23.telnet.banner.banner is not NULL
 * AND   autonomous_system.asn = 9318;
 **/

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
/**
 * SELECT COUNT(*)
 * FROM  ipv4.20160425
 * WHERE p80.http.get.body CONTAINS 'content=\"WordPress 4.0';
 **/

json_passed_t zakir_q2_p80_http_get_body(const char *value, void *) {
    if (strstr(value, "content=\"WordPress 4.0")) {
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
    static const char *_1 = "WordPress 4.0";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ ZAKIR QUERY 3 **************************
/**
 * SELECT COUNT(*)
 * FROM  ipv4.20160425
 * WHERE autonomous_system.asn = 2516;
 **/

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
/**
 * SELECT COUNT(*)
 * FROM  ipv4.20160425
 * WHERE location.country = "Chile"
 * AND   p80.http.get.status_code is not NULL;
 **/

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
/**
 * SELECT COUNT(*)
 * FROM ipv4.20160425
 * WHERE p80.http.get.headers.server like '%DIR-300%';
 **/

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
/**
 * SELECT COUNT(*)
 * FROM ipv4.20160425
 * WHERE p110.pop3s.starttls.banner is not NULL;
 **/

// Checking for nullity
json_passed_t zakir_q6_p110_pop3_starttls_banner(const char *, void *) {
    return JSON_PASS;
}

json_query_t zakir_query6() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "p110.pop3.starttls.banner",
                                 zakir_q6_p110_pop3_starttls_banner);
    return query;
}

static const char **sparser_zakir_query6(int *count) {
    static const char *_1 = "p110";
    static const char *_2 = "pop3s";
    static const char *_3 = "starttls";
    static const char *_4 = "banner";
    static const char *predicates[] = {_1, _2, _3, _4, NULL};

    *count = 4;
    return predicates;
}

// ************************ ZAKIR QUERY 7 **************************
/**
 * SELECT COUNT(*)
 * FROM ipv4.20160425
 * WHERE p21.ftp.banner.banner like '%Seagate Central Shared%';
 **/

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
/**
 * SELECT COUNT(*)
 * FROM ipv4.20160425
 * WHERE p20000.dnp3.status.support = true;
 **/

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
    static const char *_3 = "status";
    static const char *_4 = "support";
    static const char *predicates[] = {_1, _2, _3, _4, NULL};

    *count = 4;
    return predicates;
}
// ************************ ZAKIR QUERY 9 **************************
/**
 * SELECT autonomous_system.asn, count(ipint) AS count
 * FROM ipv4.20160425
 * WHERE autonomous_system.name CONTAINS 'Verizon'
 * GROUP BY autonomous_system.asn;
 **/

typedef struct zakir_q9_proj {
    int asn;
    int ipint;
} zakir_q9_proj_t;

// Checking nullity
json_passed_t zakir_q9_autonomoussystem_name(const char *value, void *) {
    return strstr(value, "Verizon") ? JSON_PASS : JSON_FAIL;
}

json_passed_t zakir_q9_autonomoussystem_asn_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    zakir_q9_proj_t *row = ((zakir_q9_proj_t *)ctx->ptr) + ctx->count;
    row->asn = value;

    return JSON_PASS;
}

json_passed_t zakir_q9_ipint_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    zakir_q9_proj_t *row = ((zakir_q9_proj_t *)ctx->ptr) + ctx->count;
    row->ipint = value;

    return JSON_PASS;
}

json_query_t zakir_query9() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "autonomous_system.name",
                                  zakir_q9_autonomoussystem_name);
    json_query_add_integer_filter(query, "autonomous_system.asn",
                                  zakir_q9_autonomoussystem_asn_proj);
    json_query_add_integer_filter(query, "ipint",
                                  zakir_q9_ipint_proj);
    return query;
}

static const char **sparser_zakir_query9(int *count) {
    static const char *_1 = "Verizon";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}


// ************************ ZAKIR QUERY 10 **************************
/**
 * SELECT autonomous_system.asn AS asn, COUNT(ipint) AS hosts
 * FROM ipv4.20160425
 * WHERE p502.modbus.device_id.function_code is not NULL
 * GROUP BY asn ORDER BY asn DESC;
 **/

typedef struct zakir_q10_proj {
    int asn;
    int ipint;
} zakir_q10_proj_t;

// Checking nullity
json_passed_t zakir_q10_p502_modbus_device_id_function_code(int64_t, void *) {
    return JSON_PASS;
}

json_passed_t zakir_q10_autonomoussystem_asn_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    zakir_q10_proj_t *row = ((zakir_q10_proj_t *)ctx->ptr) + ctx->count;
    row->asn = value;

    return JSON_PASS;
}

json_passed_t zakir_q10_ipint_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    zakir_q10_proj_t *row = ((zakir_q10_proj_t *)ctx->ptr) + ctx->count;
    row->ipint = value;

    return JSON_PASS;
}

json_query_t zakir_query10() {
    json_query_t query = json_query_new();
    json_query_add_integer_filter(query, "p502.modbus.device_id.function_code",
                                  zakir_q10_p502_modbus_device_id_function_code);
    json_query_add_integer_filter(query, "autonomous_system.asn",
                                  zakir_q10_autonomoussystem_asn_proj);
    json_query_add_integer_filter(query, "ipint",
                                  zakir_q10_ipint_proj);
    return query;
}

static const char **sparser_zakir_query10(int *count) {
    static const char *_1 = "p502";
    static const char *_2 = "modbus";
    static const char *_3 = "device_id";
    static const char *_4 = "function_code";
    static const char *predicates[] = {_1, _2, _3, _4, NULL};

    *count = 4;
    return predicates;
}

// ************************ TWITTER QUERY 1 **************************
/**
 * SELECT count(*)
 * FROM tweets
 * WHERE text contains "Donald Trump"
 * AND created_at contains "Sep 13";
 **/

json_passed_t twitter_q1_text(const char *value, void *) {
    return strstr(value, "Donald Trump") ? JSON_PASS : JSON_FAIL;
}

json_passed_t twitter_q1_created_at(const char *value, void *) {
    return strstr(value, "Sep 13") ? JSON_PASS : JSON_FAIL;
}

json_query_t twitter_query1() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "text", twitter_q1_text);
    json_query_add_string_filter(query, "created_at", twitter_q1_created_at);
    return query;
}

static const char **sparser_twitter_query1(int *count) {
    static const char *_1 = "Donald Trump";
    static const char *_2 = "Sep 13";
    static const char *predicates[] = {_1, _2, NULL};

    *count = 2;
    return predicates;
}

// ************************ TWITTER QUERY 2 **************************
/**
 * SELECT user.id, SUM(retweet_count)
 * FROM tweets
 * WHERE text contains "Obama"
 * GROUP BY user.id;
 **/

typedef struct twitter_q2_proj {
    long user_id;
    int retweet_count;
} twitter_q2_proj_t;

json_passed_t twitter_q2_text(const char *value, void *) {
    return strstr(value, "Obama") ? JSON_PASS : JSON_FAIL;
}

json_passed_t twitter_q2_user_id_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    twitter_q2_proj_t *row = ((twitter_q2_proj_t *)ctx->ptr) + ctx->count;
    row->user_id = value;

    return JSON_PASS;
}

json_passed_t twitter_q2_retweet_count_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    twitter_q2_proj_t *row = ((twitter_q2_proj_t *)ctx->ptr) + ctx->count;
    row->retweet_count = value;

    return JSON_PASS;
}

json_query_t twitter_query2() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "text", twitter_q2_text);
    json_query_add_integer_filter(query, "user.id", twitter_q2_user_id_proj);
    json_query_add_integer_filter(query, "retweet_count",
                                  twitter_q2_retweet_count_proj);
    return query;
}

static const char **sparser_twitter_query2(int *count) {
    static const char *_1 = "Obama";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ TWITTER QUERY 3 **************************
/**
 * SELECT id
 * FROM tweets
 * WHERE user.lang = "msa";
 **/

typedef struct twitter_q3_id_proj { long id; } twitter_q3_proj_t;

json_passed_t twitter_q3_user_lang(const char *value, void *) {
    return (strcmp(value, "msa") == 0) ? JSON_PASS : JSON_FAIL;
}

json_passed_t twitter_q3_id_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    twitter_q3_proj_t *row = ((twitter_q3_proj_t *)ctx->ptr) + ctx->count;
    row->id = value;

    return JSON_PASS;
}

json_query_t twitter_query3() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "user.lang", twitter_q3_user_lang);
    json_query_add_integer_filter(query, "id", twitter_q3_id_proj);
    return query;
}

static const char **sparser_twitter_query3(int *count) {
    static const char *_1 = "msa";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************************ TWITTER QUERY 4 **************************
/**
 * SELECT distinct user.id
 * FROM tweets
 * WHERE text contains @realDonaldTrump;
 **/

typedef struct twitter_q4_proj { long user_id; } twitter_q4_proj_t;

json_passed_t twitter_q4_text(const char *value, void *) {
    return strstr(value, "@realDonaldTrump") ? JSON_PASS : JSON_FAIL;
}

json_passed_t twitter_q4_user_id_proj(int64_t value, void *data) {
    callback_info_t *ctx = (callback_info_t *)data;
    twitter_q4_proj_t *row = ((twitter_q4_proj_t *)ctx->ptr) + ctx->count;
    row->user_id = value;

    return JSON_PASS;
}

json_query_t twitter_query4() {
    json_query_t query = json_query_new();
    json_query_add_string_filter(query, "text", twitter_q4_text);
    json_query_add_integer_filter(query, "user.id", twitter_q4_user_id_proj);
    return query;
}

static const char **sparser_twitter_query4(int *count) {
    static const char *_1 = "@realDonaldTrump";
    static const char *predicates[] = {_1, NULL};

    *count = 1;
    return predicates;
}

// ************** All the queries we want to test **************
const zakir_query_t queries[] = {zakir_query1,
                                 zakir_query2,
                                 zakir_query3,
                                 zakir_query4,
                                 zakir_query5,
                                 zakir_query6,
                                 zakir_query7,
                                 zakir_query8,
                                 zakir_query9,
                                 zakir_query10,
                                 twitter_query1,
                                 twitter_query2,
                                 twitter_query3,
                                 twitter_query4,
                                 NULL};
const sparser_zakir_query_preds_t squeries[] = {sparser_zakir_query1,
                                                sparser_zakir_query2,
                                                sparser_zakir_query3,
                                                sparser_zakir_query4,
                                                sparser_zakir_query5,
                                                sparser_zakir_query6,
                                                sparser_zakir_query7,
                                                sparser_zakir_query8,
                                                sparser_zakir_query9,
                                                sparser_zakir_query10,
                                                sparser_twitter_query1,
                                                sparser_twitter_query2,
                                                sparser_twitter_query3,
                                                sparser_twitter_query4,
                                                NULL};

#endif
