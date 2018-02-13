
#include "bench_parquet.h"

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

using std::cout;
using std::endl;
using std::shared_ptr;
using namespace parquet;

int main(int, char *argv[]) {
    const char *filename = argv[1];
    const int query_field_index = atoi(argv[2]);
    const char *query_str = argv[3];
    const char *query_substr = argv[4];
    char *raw;
    const size_t file_length = read_all(filename, &raw);
    cout << "File length: " << file_length << endl;

    parquet_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    ctx.query_str = query_str;
    ctx.query_field_index = query_field_index;
    parquet_iterator_t itr;
    memset(&itr, 0, sizeof(itr));

    // Create a ParquetReader instance
    itr.parquet_reader = ParquetFileReader::OpenFile(filename, false);
    itr.curr_byte_offset = 4;
    itr.start = raw + 4;  // skip magic 4-byte string
    ctx.itr = &itr;

    // Add the query
    sparser_query_t *query =
        (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
    sparser_add_query_binary(query, query_substr, 4);

    std::shared_ptr<FileMetaData> file_metadata =
        itr.parquet_reader->metadata();
    const auto group_metadata = file_metadata->RowGroup(0);
    const auto row_group_reader = itr.parquet_reader->RowGroup(0);
    auto column_chunk = group_metadata->ColumnChunk(ctx.query_field_index);

    // Benchmark Sparser
    bench_timer_t s = time_start();
    sparser_stats_t *stats = sparser_search4_binary(
        raw + column_chunk->data_page_offset(),
        column_chunk->total_compressed_size(), query, verify_parquet, &ctx);
    assert(stats);

    double parse_time = time_stop(s);
    cout << sparser_format_stats(stats) << endl;
    cout << "Total Runtime: " << parse_time << " seconds" << endl;

    s = time_start();
    verify_parquet_loop(&ctx);
    parse_time = time_stop(s);
    printf("Loop Runtime: %f seconds\n", parse_time);

    free(query);
    free(stats);
    free(raw);

    return 0;
}
