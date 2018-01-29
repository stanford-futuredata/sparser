
#include "bench_avro.h"

/* The Chicago Taxi dataset has the following Avro schema:
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

int main(int, char *argv[]) {
    const char *filename = argv[1];
    const int query_field_index = atoi(argv[2]);
    const char *query_str =
        argv[3];  //"de5c1de70947da44115c5b8e4073f302040ff570";
    const char *query_substr = argv[4];  //"de5c";
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
    read_int64(&raw, &itr.num_records);
    read_int64(&raw, &itr.num_bytes);
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
    read_int64(&raw, &itr.num_records);
    read_int64(&raw, &itr.num_bytes);
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
