
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

    parquet_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    ctx.query_str = query_str;
    ctx.query_field_index = query_field_index;
    parquet_iterator_t itr;
    memset(&itr, 0, sizeof(itr));

    // Create a ParquetReader instance
    itr.parquet_reader = ParquetFileReader::OpenFile(filename, false);
    ctx.itr = &itr;

    // Add the query
    sparser_query_t *query =
        (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
    sparser_add_query_binary(query, query_substr, 4);

    std::shared_ptr<FileMetaData> file_metadata =
        itr.parquet_reader->metadata();

    // Benchmark Sparser
    sparser_stats_t global_stats;
    memset(&global_stats, 0, sizeof(global_stats));
    bench_timer_t s = time_start();
    for (int r = 0; r < file_metadata->num_row_groups(); ++r) {
        const auto group_metadata = file_metadata->RowGroup(r);
        const auto row_group_reader = itr.parquet_reader->RowGroup(r);
        const auto column_chunk =
            group_metadata->ColumnChunk(ctx.query_field_index);

        std::shared_ptr<ColumnReader> col_reader =
            row_group_reader->Column(ctx.query_field_index);
        parquet::ByteArrayReader *ba_reader =
            static_cast<parquet::ByteArrayReader *>(col_reader.get());
        while (ba_reader->ReadNewPage()) {
            std::shared_ptr<Page> current_page = ba_reader->current_page();
            const DataPage *page =
                static_cast<const DataPage *>(current_page.get());
            ctx.itr->curr_ptr = (char *)page->data();
#ifdef DEBUG
            cout << page->num_values() << endl;
#endif
            initialize_ptr_in_data_page(&ctx.itr->curr_ptr);
            ctx.itr->num_rows_so_far = 0;
            ctx.itr->num_rows_in_page = page->num_values();
            sparser_stats_t *stats =
                sparser_search4_binary((char *)page->data(), page->size(),
                                       query, verify_parquet, &ctx);
            assert(stats);
#ifdef DEBUG
            cout << sparser_format_stats(stats) << endl;
#endif
            global_stats.sparser_passed += stats->sparser_passed;
            global_stats.callback_passed += stats->callback_passed;
        }
#ifdef DEBUG
        cout << endl;
#endif
    }

    double parse_time = time_stop(s);
    global_stats.fraction_passed_correct =
        (double)global_stats.callback_passed /
        (double)global_stats.sparser_passed;
    global_stats.fraction_passed_incorrect =
        1.0 - global_stats.fraction_passed_correct;
    cout << sparser_format_stats(&global_stats) << endl;
    cout << "Sparser Runtime: " << parse_time << " seconds" << endl;

    s = time_start();
    verify_parquet_loop(&ctx);
    parse_time = time_stop(s);
    printf("Loop Runtime: %f seconds\n", parse_time);

    free(query);

    return 0;
}
