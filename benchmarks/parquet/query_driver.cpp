
#include "bench_parquet.h"

int main(int, char *argv[]) {
    const char *filename = argv[1];
    const int query_field_index = atoi(argv[2]);
    const char *query_str = argv[3];
    const char *query_substr = argv[4];
    char *raw;
    const size_t file_length = read_all(filename, &raw);
    char *init = raw;

    avro_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    ctx.schema = twitter_schema(&ctx.num_schema_elems);
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
    itr.ptr = raw;

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
    itr.ptr = raw;

    s = time_start();
    verify_avro_loop(&ctx);
    parse_time = time_stop(s);
    printf("Loop Runtime: %f seconds\n", parse_time);

    free(query);
    // free(stats);
    free(init);

    return 0;
}
