#ifndef _ZAKIR_QUERIES_H_
#define _ZAKIR_QUERIES_H_

#include "zakir_common.h"

typedef json_query_t (*zakir_query_t)();
typedef const char ** (*sparser_query_preds_t)(int *);

#define ZAKIR_BENCH_SPARSER

// All the queries we want to test.
const zakir_query_t queries[] = {
  zakir_query1,
  zakir_query2,
  zakir_query3,
  zakir_query4,
  zakir_query5,
  zakir_query6,
  zakir_query7,
  zakir_query8,
  NULL
};

// All the queries we want to test.
const sparser_query_preds_t squeries[] = {
  sparser_query1,
  sparser_query2,
  sparser_query3,
  sparser_query4,
  sparser_query5,
  sparser_query6,
  sparser_query7,
  sparser_query8,
  NULL
};

// *************************** QUERY 1  ****************************************

// Just checking for NULL.
json_passed_t q1_p23_telnet_banner_banner(const char *value, void *data) {
  return JSON_PASS;
}

json_passed_t q1_autonomoussystem_asn(int64_t value, void *data) {
  return value == 9318 ? JSON_PASS : JSON_FAIL;
}

json_query_t
zakir_query1() {
  json_query_t query = json_query_new();
  json_query_add_string_filter(query, "p23.telnet.banner.banner", q1_p23_telnet_banner_banner);
  json_query_add_integer_filter(query, "autonomous_system.asn", q1_autonomoussystem_asn);
  return query;
}

static const char **
sparser_query1(int *count) {
  static const char *_1 = "9318";
  static const char *predicates[] = {
    _1,
    NULL
  };

  *count = 1;
  return predicates;
}


// *************************** QUERY 2  ****************************************

json_passed_t q2_p80_http_get_body(const char *value, void *data) {
  if (strstr(value, "content=\"wordpress 3.5.1")) {
    return JSON_PASS;
  }
  return JSON_FAIL;
}

json_query_t
zakir_query2() {
  json_query_t query = json_query_new();
  json_query_add_string_filter(query, "p80.http.get.body", q2_p80_http_get_body);
  return query;
}

static const char **
sparser_query2(int *count) {
  static const char *_1 = "wordpress 3.5.1";
  static const char *predicates[] = {
    _1,
    NULL
  };

  *count = 1;
  return predicates;
}


// *************************** QUERY 3  ****************************************


json_passed_t q3_autonomoussystem_asn(int64_t value, void *data) {
  return (value == 2516) ? JSON_PASS : JSON_FAIL;
}

json_query_t
zakir_query3() {
  json_query_t query = json_query_new();
  json_query_add_integer_filter(query, "autonomous_system.asn", q3_autonomoussystem_asn);
  return query;
}

static const char **
sparser_query3(int *count) {
  static const char *_1 = "2516";
  static const char *predicates[] = {
    _1,
    NULL
  };

  *count = 1;
  return predicates;
}



// *************************** QUERY 4  ****************************************

json_passed_t q4_location_country(const char *value, void *data) {
  return (strcmp(value, "Chile") == 0) ? JSON_PASS : JSON_FAIL;
}

// Just checking for nullity
json_passed_t q4_p80_http_get_statuscode(int64_t value, void *data) {
  return JSON_PASS;
}

json_query_t
zakir_query4() {
  json_query_t query = json_query_new();
  json_query_add_string_filter(query, "location.country", q4_location_country);
  json_query_add_integer_filter(query, "p80.http.get.status_code", q4_p80_http_get_statuscode);
  return query;
}


static const char **
sparser_query4(int *count) {
  static const char *_1 = "Chile";
  static const char *_2 = "status_code";
  static const char *predicates[] = {
    _1,
    _2,
    NULL
  };

  *count = 2;
  return predicates;
}

// *************************** QUERY 5  ****************************************


json_passed_t q5_p80_http_get_headers_server(const char *value, void *data) {
  return  (strstr(value, "DIR-300") == 0) ? JSON_PASS : JSON_FAIL;
}

json_query_t
zakir_query5() {
  json_query_t query = json_query_new();
  json_query_add_string_filter(query, "p80.http.get.headers.server", q5_p80_http_get_headers_server);
  return query;
}

static const char **
sparser_query5(int *count) {
  static const char *_1 = "DIR-300";
  static const char *predicates[] = {
    _1,
    NULL
  };

  *count = 1;
  return predicates;
}


// *************************** QUERY 6  ****************************************


// Checking for nullity
json_passed_t q6_p995_pop3s_tls_banner(const char *value, void *data) {
  return JSON_PASS;
}


// Checking for nullity
json_passed_t q6_p110_pop3_starttls_banner(const char *value, void *data) {
  return  JSON_PASS;
}


json_query_t
zakir_query6() {
  json_query_t query = json_query_new();
  json_query_add_string_filter(query, "p110.pop3.starttls.banner", q6_p110_pop3_starttls_banner);
  json_query_add_string_filter(query, "p995.pop3s.tls.banner", q6_p995_pop3s_tls_banner);
  return query;
}

static const char **
sparser_query6(int *count) {
  static const char *_1 = "p110";
  static const char *_2 = "p995";
  static const char *_3 = "pop3s";
  static const char *_4 = "starttls";
  static const char *predicates[] = {
    _1,
    _2,
    _3,
    _4,
    NULL
  };

  *count = 4;
  return predicates;
}


// *************************** QUERY 7  ****************************************


// Checking for nullity
json_passed_t q7_ftp_banner_banner(const char *value, void *data) {
  return strstr(value, "Seagate Central Shared") ? JSON_PASS : JSON_FAIL;
}


json_query_t
zakir_query7() {
  json_query_t query = json_query_new();
  json_query_add_string_filter(query, "ftp.banner.banner", q7_ftp_banner_banner);
  return query;
}

static const char **
sparser_query7(int *count) {
  static const char *_1 = "Seagate Central Shared";
  static const char *predicates[] = {
    _1,
    NULL
  };

  *count = 1;
  return predicates;
}


// *************************** QUERY 7  ****************************************

// Checking for nullity
json_passed_t q8_p20000_dnp3_status_support(bool value, void *data) {
  return value ? JSON_PASS : JSON_FAIL;
}


json_query_t
zakir_query8() {
  json_query_t query = json_query_new();
  json_query_add_boolean_filter(query, "p20000.dnp3.status.support", q8_p20000_dnp3_status_support);
  return query;
}


static const char **
sparser_query8(int *count) {
  static const char *_1 = "p20000";
  static const char *_2 = "dnp3";
  static const char *predicates[] = {
    _1,
    _2,
    NULL
  };

  *count = 2;
  return predicates;
}

#endif
