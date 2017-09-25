/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <avro.h>
#include <endian.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "sparser.h"

#ifdef DEFLATE_CODEC
#define QUICKSTOP_CODEC "deflate"
#else
#define QUICKSTOP_CODEC "null"
#endif

/**** AVRO QUERIES ***/
/**
 * SELECT AVG(trip_seconds)
 * FROM   chicago-taxi
 * WHERE  company = 'Choice Taxi Association';
**/
/**
 * SELECT MAX(fare) AS maximum_fare, MIN(fare) AS minimum_fare, AVG(fare) AS
 *        avg_fare, STDDEV(fare) AS std_dev_fare
 * FROM   chicago-taxi
 * WHERE  dropoff_census_tract = 17031320100;
 **/
/**
 * SELECT MAX(tips)
 * FROM   chicago-taxi
 * WHERE  company = 'Taxi Affiliation Services'
 * AND    payment_type = 'Credit Card';
 **/

float int_bits_to_float(const uint32_t bits) {
    const int sign = ((bits >> 31) == 0) ? 1 : -1;
    const int exponent = ((bits >> 23) & 0xff);
    const int mantissa =
        (exponent == 0) ? (bits & 0x7fffff) << 1 : (bits & 0x7fffff) | 0x800000;
    return (sign * mantissa * pow(2, exponent - 150));
}

double long_bits_to_double(const uint64_t bits) {
    const int sign = ((bits >> 63) == 0) ? 1 : -1;
    const int exponent = (int)((bits >> 52) & 0x7ffL);
    const long mantissa = (exponent == 0)
                              ? (bits & 0xfffffffffffffL) << 1
                              : (bits & 0xfffffffffffffL) | 0x10000000000000L;
    return (sign * mantissa * pow(2, exponent - 1075));
}

#define MAX_VARINT_BUF_SIZE 10
int encode_long(char *outer_buf, int64_t l) {
    char buf[MAX_VARINT_BUF_SIZE];
    uint8_t bytes_written = 0;
    uint64_t n = (l << 1) ^ (l >> 63);
    while (n & ~0x7F) {
        buf[bytes_written++] = (char)((((uint8_t)n) & 0x7F) | 0x80);
        n >>= 7;
    }
    buf[bytes_written++] = (char)n;
    strncpy(outer_buf, buf, bytes_written);
    return 0;
}

int64_t read_long_debug(char *buf) {
    uint64_t value = 0;
    uint8_t b;
    int offset = 0;
    do {
        if (offset == MAX_VARINT_BUF_SIZE) {
            /*
             * illegal byte sequence
             */
            avro_set_error("Varint too long");
            return EILSEQ;
        }
        b = (uint8_t)*buf;
        ++buf;
        value |= (int64_t)(b & 0x7F) << (7 * offset);
        ++offset;
    } while (b & 0x80);
    int64_t ret = ((value >> 1) ^ -(value & 1));
    return ret;
}

uint32_t read_little_endian_four_bytes(char **outer_buf) {
    uint32_t *valptr = (uint32_t *)*outer_buf;
    uint32_t value = *valptr;
    *outer_buf = (char *)(valptr + 1);
    return htole32(value);
}

uint64_t read_little_endian_eight_bytes(char **outer_buf) {
    uint64_t *valptr = (uint64_t *)*outer_buf;
    uint64_t value = *valptr;
    *outer_buf = (char *)(valptr + 1);
    return htole64(value);
}

int read_long(char **outer_buf, int64_t *l) {
    char *buf = *outer_buf;
    uint64_t value = 0;
    uint8_t b;
    int offset = 0;
    do {
        if (offset == MAX_VARINT_BUF_SIZE) {
            /*
             * illegal byte sequence
             */
            avro_set_error("Varint too long");
            return EILSEQ;
        }
        b = (uint8_t)*buf;
        ++buf;
        value |= (int64_t)(b & 0x7F) << (7 * offset);
        ++offset;
    } while (b & 0x80);
    *l = ((value >> 1) ^ -(value & 1));
    *outer_buf = buf;
    return 0;
}

/*
"fields":[
  {"name":"unique_key","type":"string"},
  {"name":"taxi_id","type":"string"},
  {"name":"trip_start_timestamp","type":["null","long"]},
  {"name":"trip_end_timestamp","type":["null","long"]},
  {"name":"trip_seconds","type":["null","long"]},
  {"name":"trip_miles","type":["null","double"]},
  {"name":"pickup_census_tract","type":["null","long"]},
  {"name":"dropoff_census_tract","type":["null","long"]},
  {"name":"pickup_community_area","type":["null","long"]},
  {"name":"dropoff_community_area","type":["null","long"]},
  {"name":"fare","type":["null","double"]},
  {"name":"tips","type":["null","double"]},
  {"name":"tolls","type":["null","double"]},
  {"name":"extras","type":["null","double"]},
  {"name":"trip_total","type":["null","double"]},
  {"name":"payment_type","type":["null","string"]},
  {"name":"company","type":["null","string"]},
  {"name":"pickup_latitude","type":["null","double"]},
  {"name":"pickup_longitude","type":["null","double"]},
  {"name":"pickup_location","type":["null","string"]},
  {"name":"dropoff_latitude","type":["null","double"]},
  {"name":"dropoff_longitude","type":["null","double"]},
  {"name":"dropoff_location","type":["null","string"]}
]
*/
static const avro_type_t **chicago_taxi_schema(int **schema_counts,
                                               int *total_count) {
    const int _total_count = 23;
    int *_schema_counts = (int *)malloc(sizeof(int) * _total_count);

    // {"name":"unique_key","type":"string"},
    static const avro_type_t _1[1] = {AVRO_STRING};
    _schema_counts[0] = 1;
    // {"name":"taxi_id","type":"string"},
    static const avro_type_t _2[1] = {AVRO_STRING};
    _schema_counts[1] = 1;
    // {"name":"trip_start_timestamp","type":["null","long"]},
    static const avro_type_t _3[2] = {AVRO_NULL, AVRO_INT64};
    _schema_counts[2] = 2;
    // {"name":"trip_end_timestamp","type":["null","long"]},
    static const avro_type_t _4[2] = {AVRO_NULL, AVRO_INT64};
    _schema_counts[3] = 2;
    // {"name":"trip_seconds","type":["null","long"]},
    static const avro_type_t _5[2] = {AVRO_NULL, AVRO_INT64};
    _schema_counts[4] = 2;
    // {"name":"trip_miles","type":["null","double"]},
    static const avro_type_t _6[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[5] = 2;
    // {"name":"pickup_census_tract","type":["null","long"]},
    static const avro_type_t _7[2] = {AVRO_NULL, AVRO_INT64};
    _schema_counts[6] = 2;
    // {"name":"dropoff_census_tract","type":["null","long"]},
    static const avro_type_t _8[2] = {AVRO_NULL, AVRO_INT64};
    _schema_counts[7] = 2;
    // {"name":"pickup_community_area","type":["null","long"]},
    static const avro_type_t _9[2] = {AVRO_NULL, AVRO_INT64};
    _schema_counts[8] = 2;
    // {"name":"dropoff_community_area","type":["null","long"]},
    static const avro_type_t _10[2] = {AVRO_NULL, AVRO_INT64};
    _schema_counts[9] = 2;
    // {"name":"fare","type":["null","double"]},
    static const avro_type_t _11[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[10] = 2;
    // {"name":"tips","type":["null","double"]},
    static const avro_type_t _12[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[11] = 2;
    // {"name":"tolls","type":["null","double"]},
    static const avro_type_t _13[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[12] = 2;
    // {"name":"extras","type":["null","double"]},
    static const avro_type_t _14[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[13] = 2;
    // {"name":"trip_total","type":["null","double"]},
    static const avro_type_t _15[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[14] = 2;
    // {"name":"payment_type","type":["null","string"]},
    static const avro_type_t _16[2] = {AVRO_NULL, AVRO_STRING};
    _schema_counts[15] = 2;
    // {"name":"company","type":["null","string"]},
    static const avro_type_t _17[2] = {AVRO_NULL, AVRO_STRING};
    _schema_counts[16] = 2;
    // {"name":"pickup_latitude","type":["null","double"]},
    static const avro_type_t _18[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[17] = 2;
    // {"name":"pickup_longitude","type":["null","double"]},
    static const avro_type_t _19[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[18] = 2;
    // {"name":"company","type":["null","string"]},
    static const avro_type_t _20[2] = {AVRO_NULL, AVRO_STRING};
    _schema_counts[19] = 2;
    // {"name":"dropoff_latitude","type":["null","double"]},
    static const avro_type_t _21[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[20] = 2;
    // {"name":"dropoff_longitude","type":["null","double"]},
    static const avro_type_t _22[2] = {AVRO_NULL, AVRO_DOUBLE};
    _schema_counts[21] = 2;
    // {"name":"dropoff_location","type":["null","string"]}
    static const avro_type_t _23[2] = {AVRO_NULL, AVRO_STRING};
    _schema_counts[22] = 2;

    static const avro_type_t *schema[] = {
        _1,  _2,  _3,  _4,  _5,  _6,  _7,  _8,  _9,  _10, _11, _12,
        _13, _14, _15, _16, _17, _18, _19, _20, _21, _22, _23, NULL};
    *schema_counts = _schema_counts;
    *total_count = _total_count;
    return schema;
}

static const avro_type_t **test_schema(int **schema_counts, int *total_count) {
    const int _total_count = 3;
    int *_schema_counts = (int *)malloc(sizeof(int) * _total_count);

    static const avro_type_t first[1] = {AVRO_STRING};
    _schema_counts[0] = 1;
    static const avro_type_t second[2] = {AVRO_INT32, AVRO_NULL};
    _schema_counts[1] = 2;
    static const avro_type_t third[2] = {AVRO_STRING, AVRO_NULL};
    _schema_counts[2] = 2;
    static const avro_type_t *schema[] = {first, second, third, NULL};

    *schema_counts = _schema_counts;
    *total_count = _total_count;
    return schema;
}

typedef struct avro_iterator {
    long num_records;
    long num_bytes;
    char *prev_header;
    char *curr_record;
    char *eof;
    int curr_record_index;
} avro_iterator_t;

typedef struct avro_context {
    avro_iterator_t *itr;
    const avro_type_t **schema;
    int *schema_counts;
    int total_schema_count;
    int query_field_index;
    const char *query_str;
} avro_context_t;

int single_record_contains(char **prev_ptr, avro_context_t *ctx) {
    char *ptr = *prev_ptr;
    const char *query_str = ctx->query_str;
    const size_t query_str_length = strlen(ctx->query_str);
    const int total_schema_count = ctx->total_schema_count;
    const int *schema_counts = ctx->schema_counts;
    const avro_type_t **schema = ctx->schema;
    const int query_field_index = ctx->query_field_index;

    int ret = 0;

    for (int i = 0; i < total_schema_count; ++i) {
        const int schema_count = schema_counts[i];
        avro_type_t curr_type;
        if (schema_count == 1) {
            curr_type = schema[i][0];
        } else {
            int64_t schema_index;
            read_long(&ptr, &schema_index);
            curr_type = schema[i][schema_index];
        }
        switch (curr_type) {
            // For all cases: if we're not yet at the field we care about
            // (`query_field_index`) then we skip over the field
            case AVRO_STRING: {
                int64_t str_length;
                read_long(&ptr, &str_length);
                if (i == query_field_index) {
                    // for string fields, we implement "CONTAINS" checks
                    char *tmp = (char *)memmem(ptr, str_length, query_str,
                                               query_str_length);
                    ret = (tmp != NULL);
                }
                ptr += str_length;
                break;
            }
            case AVRO_INT32:
            case AVRO_INT64: {
                // we can't skip over ints and longs, since they're
                // variable-length encoded
                int64_t val;
                read_long(&ptr, &val);
                if (i == query_field_index) {
                    // for int and long fields, we implement full equality
                    // checks
                    ret = (val == (int64_t)query_str);
                }
                break;
            }
            case AVRO_DOUBLE: {
                if (i == query_field_index) {
                    ret = strncmp(ptr, query_str, 8) == 0;
                }
                ptr += 8;
                break;
            }
            case AVRO_FLOAT: {
                if (i == query_field_index) {
                    ret = strncmp(ptr, query_str, 4) == 0;
                }
                ptr += 4;
                break;
            }
            case AVRO_NULL:
            default:;
                // do nothing
        }
    }
    // if, somehow, we reachthis point (the query_field_index was larger than
    // the number of fields) return false
    *prev_ptr = ptr;
    return ret;
}

/**
 * Determine if a record contains a match for `ctx->query_str`. `line` should
 * fall somewhere in between the middle of a record; we assume that `ctx->itr`
 * points to the beginning of an Avro record, according to the schema defined
 * in `ctx->schema. If we find the record that contains `line` and it also
 * matches the `query_str`, return True; else return False.
 **/
int record_contains(avro_context_t *ctx, const char *line) {
    const int total_schema_count = ctx->total_schema_count;
    const int *schema_counts = ctx->schema_counts;
    const avro_type_t **schema = ctx->schema;
    avro_iterator_t *itr = ctx->itr;

    // First, find the record we need to check. Traverse through each record,
    // until we pass `line`.  Once we pass it, that means the record we
    // just processed needs to checked; go back and check it.
    char *prev_record = itr->curr_record;
    bool check = false;
    while (itr->curr_record_index < itr->num_records) {
        for (int i = 0; i < total_schema_count; ++i) {
            const int schema_count = schema_counts[i];
            avro_type_t curr_type;
            if (schema_count == 1) {
                curr_type = schema[i][0];
            } else {
                int64_t schema_index;
                read_long(&itr->curr_record, &schema_index);
                curr_type = schema[i][schema_index];
            }
            // don't read values for now, just skip over them
            switch (curr_type) {
                case AVRO_STRING: {
                    int64_t str_length;
                    read_long(&itr->curr_record, &str_length);
                    itr->curr_record += str_length;
                    break;
                }
                case AVRO_INT32:
                case AVRO_INT64: {
                    // we can't skip over ints and longs, since they're
                    // variable-length encoded
                    int64_t val;
                    read_long(&itr->curr_record, &val);
                    break;
                }
                case AVRO_DOUBLE: {
                    itr->curr_record += 8;
                    break;
                }
                case AVRO_FLOAT: {
                    itr->curr_record += 4;
                    break;
                }
                case AVRO_NULL:
                default:;
                    // do nothing
            }
        }
        itr->curr_record_index++;
        if (itr->curr_record > line) {
            check = true;
            break;
        }
        prev_record = itr->curr_record;
    }
    if (check) return single_record_contains(&prev_record, ctx);
    return 0;
}

int advance_iterator(avro_iterator_t *itr) {
    itr->curr_record = itr->prev_header + itr->num_bytes;
    itr->curr_record += 16;  // skip over 16-byte magic string
    if (itr->curr_record >= itr->eof) {
        return 0;
    }
    read_long(&itr->curr_record, &itr->num_records);
    read_long(&itr->curr_record, &itr->num_bytes);
    itr->prev_header = itr->curr_record;
    itr->curr_record_index = 0;
    return 1;
}

int verify_avro(const char *line, void *thunk) {
    if (!thunk) return 0;

    avro_context_t *ctx = (avro_context_t *)thunk;
    avro_iterator_t *itr = ctx->itr;

    // Case 1: line is behind current packet, which is weird. Abort.
    intptr_t diff = (intptr_t)line - (intptr_t)itr->curr_record;
    if (diff < 0) {
        fprintf(stderr, "current packet is behind line!\n");
        return 0;
    }

    // Case 2: line is ahead of current packet. Skip forward until we
    // encapsulate that packet,
    // and then parse it.
    while (itr->prev_header + itr->num_bytes < line) {
        advance_iterator(itr);
    }

    return record_contains(ctx, line);
}

void verify_avro_loop(avro_context_t *ctx) {
    long count = 0;
    long total = 0;

    avro_iterator_t *itr = ctx->itr;

    do {
        while (itr->curr_record_index < itr->num_records) {
            if (single_record_contains(&itr->curr_record, ctx)) {
                count++;
            }
            itr->curr_record_index++;
            total++;
        }
    } while (advance_iterator(itr));
    printf("%s count - %ld\n", ctx->query_str, count);
    printf("total count - %ld\n", total);
}

int main(int, char *argv[]) {
    const char *filename = argv[1];
    const int query_field_index = atoi(argv[2]);
    const char *query_str = argv[3]; //"de5c1de70947da44115c5b8e4073f302040ff570";
    const char *query_substr = argv[4]; //"de5c";
    char *raw;
    const size_t file_length = read_all(filename, &raw);
    char *init = raw;

    avro_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    ctx.schema =
        chicago_taxi_schema(&ctx.schema_counts, &ctx.total_schema_count);
    ctx.query_str = query_str;
    ctx.query_field_index = query_field_index;
    avro_iterator_t itr;
    memset(&itr, 0, sizeof(itr));
    ctx.itr = &itr;

    itr.eof = raw + file_length;
    // skip schema, null character, and magic 16-byte string
    raw += strlen(raw) + 1 + 16;
    char *after_header = raw;
    read_long(&raw, &itr.num_records);
    read_long(&raw, &itr.num_bytes);
    itr.prev_header = raw;
    itr.curr_record = raw;

    // Add the query
    sparser_query_t *query =
        (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
    sparser_add_query_binary(query, query_substr, 4);

    // Benchmark Sparser
    bench_timer_t s = time_start();
    sparser_stats_t *stats =
        sparser_search4_binary(raw, file_length, query, verify_avro, &ctx);
    assert(stats);
    double parse_time = time_stop(s);

    printf("%s\n", sparser_format_stats(stats));
    printf("Total Runtime: %f seconds\n", parse_time);

    // Reinitialize iterator at the beginning of the file
    raw = after_header;
    read_long(&raw, &itr.num_records);
    read_long(&raw, &itr.num_bytes);
    itr.prev_header = raw;
    itr.curr_record = raw;

    s = time_start();
    verify_avro_loop(&ctx);
    parse_time = time_stop(s);
    printf("Loop Runtime: %f seconds\n", parse_time);

    free(ctx.schema_counts);
    free(query);
    free(stats);
    free(init);

    return 0;
}
