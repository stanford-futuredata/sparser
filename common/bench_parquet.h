#ifndef _BENCH_PARQUET_H_
#define _BENCH_PARQUET_H_

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <string.h>
#include <vector>
#include "common.h"
#include "portable_endian.h"
#include "sparser.h"

using namespace parquet;
using std::cout;
using std::endl;

typedef struct parquet_iterator {
    std::unique_ptr<ParquetFileReader> parquet_reader;
    int64_t curr_byte_offset;
    int curr_row_group;
    int num_rows_so_far;
    const uint8_t *prev_value_ptr;
    parquet::ByteArrayReader *ba_reader;
    std::shared_ptr<ColumnReader> column_reader;
    char *start;
} parquet_iterator_t;

typedef struct parquet_context {
    parquet_iterator_t *itr;
    int query_field_index;
    const char *query_str;
} parquet_context_t;

void verify_parquet_loop(parquet_context_t *ctx) {
    long count = 0;
    long total = 0;

    parquet_iterator_t *itr = ctx->itr;
    std::shared_ptr<FileMetaData> file_metadata =
        itr->parquet_reader->metadata();
    for (int r = 0; r < file_metadata->num_row_groups(); ++r) {
        const auto group_metadata = file_metadata->RowGroup(r);
        const auto row_group_reader = itr->parquet_reader->RowGroup(r);
        const int c = ctx->query_field_index;
        auto column_chunk = group_metadata->ColumnChunk(c);

        std::shared_ptr<ColumnReader> column_reader =
            row_group_reader->Column(c);
        parquet::ByteArrayReader *ba_reader =
            static_cast<parquet::ByteArrayReader *>(column_reader.get());
        parquet::ByteArray value;
        while (ba_reader->HasNext()) {
            int64_t values_read;
            try {
                ba_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
            } catch (const std::exception &e) {
                std::cerr << "Parquet read error: " << e.what() << std::endl;
                printf("%s count - %ld\n", ctx->query_str, count);
                printf("total count - %ld\n", total);
                return;
            }
#ifdef DEBUG
            char printbuf[value.len + 1];
            strncpy(printbuf, (char *)value.ptr, value.len);
            printbuf[value.len] = '\0';
            printf("%s\n", printbuf);
#endif
            // check value to see if it contains query string
            char *tmp = (char *)memmem(value.ptr, value.len, ctx->query_str,
                                       strlen(ctx->query_str));
            if (tmp != NULL) {
                count++;
            }
            total++;
        }
    }
    printf("%s count - %ld\n", ctx->query_str, count);
    printf("total count - %ld\n", total);
}

int verify_parquet(const char *str, void *thunk) {
    if (!thunk) return 0;

    parquet_context_t *ctx = (parquet_context_t *)thunk;
    parquet_iterator_t *itr = ctx->itr;
    auto start = itr->start;

    const int64_t byte_offset_of_str =
        (int64_t)((intptr_t)str - (intptr_t)start);

    // Case 1: str is behind current packet, which is weird. Abort.
    if (byte_offset_of_str < itr->curr_byte_offset) {
        fprintf(stderr, "current packet is behind str!\n");
        return 0;
    }

    // Get the File MetaData
    std::shared_ptr<FileMetaData> file_metadata =
        itr->parquet_reader->metadata();

    for (int r = itr->curr_row_group; r < file_metadata->num_row_groups();
         ++r) {
        // First, find the row group that contains `str`
        if (r != itr->curr_row_group) {
            itr->curr_row_group = r;
            itr->num_rows_so_far = 0;
            itr->prev_value_ptr = nullptr;
        }
        const auto group_metadata = file_metadata->RowGroup(r);
        const auto row_group_size = group_metadata->total_byte_size();
        const auto row_group_reader = itr->parquet_reader->RowGroup(r);

        if (itr->curr_byte_offset <= byte_offset_of_str &&
            byte_offset_of_str <= itr->curr_byte_offset + row_group_size) {
            // We found the row group
            const int c = ctx->query_field_index;
            auto column_chunk = group_metadata->ColumnChunk(c);

#ifdef DEBUG
            cout << "Found row: " << r << ", col: " << c << endl;
#endif

            if (itr->num_rows_so_far == 0) {
                // row group must have changed, so we need to update the
                // iterator's current byte offset
                itr->curr_byte_offset =
                    column_chunk->data_page_offset() +
                    15;  // some weird padding, perhaps the page header
                itr->column_reader = row_group_reader->Column(c);
                itr->ba_reader = static_cast<parquet::ByteArrayReader *>(
                    itr->column_reader.get());
            }

            int64_t values_read;
            const int64_t batch_size = 1;
            parquet::ByteArray value;
#ifdef DEBUG
            assert(itr->curr_byte_offset < byte_offset_of_str);
#endif
            while (itr->ba_reader->HasNext()) {
                if (itr->num_rows_so_far >= column_chunk->num_values()) {
                    break;
                }
                if (itr->curr_byte_offset > byte_offset_of_str) {
                    // we must have passed it in the previous iteration, so
                    // stop searching
                    break;
                }
// Read one value at a time. (See `batch_size`). The number
// of rows read is returned. `values_read` contains the
// number of non-null rows
#ifdef DEBUG
                int64_t rows_read;
                try {
                    rows_read = itr->ba_reader->ReadBatch(
                        batch_size, nullptr, nullptr, &value, &values_read);
                } catch (const std::exception &e) {
                    std::cerr << "Parquet read error: " << e.what()
                              << std::endl;
                    return 0;
                }
                // Ensure only one value is read
                assert(rows_read == batch_size);
                assert(values_read == batch_size);
                cout << "Num rows so far: " << itr->num_rows_so_far << endl;
                char printbuf[value.len + 1];
                strncpy(printbuf, (char *)value.ptr, value.len);
                printbuf[value.len] = '\0';
                printf("%s\n", printbuf);
                if (value.ptr > itr->prev_value_ptr) {
                    char *test = (char *)memmem(
                        value.ptr, value.len, start + itr->curr_byte_offset + 4,
                        value.len);
                    assert(test != NULL);
                }
#else
                try {
                    itr->ba_reader->ReadBatch(batch_size, nullptr, nullptr,
                                              &value, &values_read);
                } catch (const std::exception &e) {
                    std::cerr << "Parquet read error: " << e.what()
                              << std::endl;
                    return 0;
                }
#endif
                int ret = 0;
                if (itr->curr_byte_offset + 4 + value.len >
                    byte_offset_of_str) {
                    // we check the value to see if it's correct once the
                    // curr_byte_offset
                    // reaches the offset of str
                    char *tmp =
                        (char *)memmem(value.ptr, value.len, ctx->query_str,
                                       strlen(ctx->query_str));
                    ret = (tmp != NULL);
                }
                itr->num_rows_so_far += batch_size;
                // advance curr_byte_offset if the value pointer also
                // advanced (if it did not,
                // we just visited a previous value)
                if (value.ptr > itr->prev_value_ptr) {
                    itr->curr_byte_offset +=
                        4 + value.len;  // each byte array (string) is 4 bytes
                                        // little endian (which encodes the
                                        // length of the string) followed by the
                                        // string itself.
                    itr->prev_value_ptr = value.ptr;
                }
                if (ret == 1) {
                    return ret;
                }
            }
            return 0;
        }
    }

    // Case 2: str is ahead of current packet. Skip forward until we
    // encapsulate that packet, and then parse it.
    // while (itr->prev_header + itr->num_bytes < str) {
    //     advance_iterator(itr);
    // }

    return 0;
}

#endif
