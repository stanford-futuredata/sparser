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
    char *curr_ptr;
    int64_t num_rows_so_far;
    int64_t num_rows_in_page;
} parquet_iterator_t;

typedef struct parquet_context {
    parquet_iterator_t *itr;
    int query_field_index;
    const char *query_str;
} parquet_context_t;

// Read 32-bit integer as four bytes little endian, and advance
// the pointer
uint32_t read_little_endian_four_bytes(char **outer_buf) {
    uint32_t *valptr = (uint32_t *)*outer_buf;
    uint32_t value = *valptr;
    *outer_buf = (char *)(valptr + 1);
    return htole32(value);
}

#ifdef DEBUG
// Read 32-bit integer as four bytes little endian, but don't
// advance the pointer
uint32_t read_little_endian_four_bytes(char *buf) {
    uint32_t *valptr = (uint32_t *)buf;
    uint32_t value = *valptr;
    return htole32(value);
}
#endif

// Initialize curr_ptr to start at beginning of the values in data page
// Initially, it seemed like you the values begin 8 bytes after the start
// of the data page; that's not always true. Instead we read four little
// endian bytes, which gives us how many bytes to skip to reach the values.
void initialize_ptr_in_data_page(char **curr_ptr) {
  const uint32_t incr = read_little_endian_four_bytes(curr_ptr);
  *curr_ptr += incr;
}

// Callback used with Sparser
int verify_parquet(const char *str, void *thunk) {
    if (!thunk) return 0;

    parquet_context_t *ctx = (parquet_context_t *)thunk;
    parquet_iterator_t *itr = ctx->itr;

    // Case 2: str is behind current record, which is weird. Abort.
    if ((intptr_t)itr->curr_ptr > (intptr_t)str) {
        fprintf(stderr, "current record is ahead of str!\n");
        return 0;
    }

    while (itr->curr_ptr <= str) {
        const uint32_t str_length =
            read_little_endian_four_bytes(&itr->curr_ptr);
        if (itr->curr_ptr + str_length > str) {
            char *test = (char *)memmem(itr->curr_ptr, str_length,
                                        ctx->query_str, strlen(ctx->query_str));
            if (test != NULL) {
#ifdef DEBUG
                char printbuf[str_length + 1];
                strncpy(printbuf, itr->curr_ptr, str_length);
                printbuf[str_length] = '\0';
                printf("%s\n", printbuf);
#endif
                itr->curr_ptr += str_length;
                return 1;
            }
        }
        itr->curr_ptr += str_length;
        itr->num_rows_so_far++;
    }
    return 0;
}

// Loop through all values and check them one by one
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
        const auto column_chunk = group_metadata->ColumnChunk(c);

        std::shared_ptr<ColumnReader> column_reader =
            row_group_reader->Column(c);
        ByteArrayReader *ba_reader =
            static_cast<ByteArrayReader *>(column_reader.get());
        ByteArray value;
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
            // check value to see if it contains query string
            char *test = (char *)memmem(value.ptr, value.len, ctx->query_str,
                                       strlen(ctx->query_str));
            if (test != NULL) {
                count++;
#ifdef DEBUG
                char printbuf[value.len + 1];
                strncpy(printbuf, (char *)value.ptr, value.len);
                printbuf[value.len] = '\0';
                printf("%s\n", printbuf);
#endif
            }
            total++;
        }
    }
    printf("%s count - %ld\n", ctx->query_str, count);
    printf("total count - %ld\n", total);
}

#endif
