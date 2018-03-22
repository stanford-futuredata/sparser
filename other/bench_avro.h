#ifndef _BENCH_AVRO_H_
#define _BENCH_AVRO_H_

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "portable_endian.h"
#include "sparser.h"

#ifdef DEFLATE_CODEC
#define QUICKSTOP_CODEC "deflate"
#else
#define QUICKSTOP_CODEC "null"
#endif

enum avro_type_t {
    AVRO_STRING,
    AVRO_BYTES,
    AVRO_INT32,
    AVRO_INT64,
    AVRO_FLOAT,
    AVRO_DOUBLE,
    AVRO_BOOLEAN,
    AVRO_NULL,
    AVRO_RECORD,
    AVRO_ENUM,
    AVRO_FIXED,
    AVRO_MAP,
    AVRO_ARRAY,
    AVRO_UNION,
    AVRO_LINK
};

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

int read_int64(char **outer_buf, int64_t *l) {
    char *buf = *outer_buf;
    uint64_t value = 0;
    uint8_t b;
    int offset = 0;
    do {
        if (offset == MAX_VARINT_BUF_SIZE) {
            /*
             * illegal byte sequence
             */
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

void read_boolean(char **outer_buf, bool *b) {
    uint8_t *buf = (uint8_t *)*outer_buf;
    *b = (*buf++ == 1) ? true : false;
    *outer_buf = (char *)buf;
}

void skip_array(char **outer_buf, avro_type_t *types) {
    char *buf = *outer_buf;
    size_t i;         /* index within the current block */
    size_t index = 0; /* index within the entire array */
    int64_t block_count;
    int64_t block_size = -1;

    read_int64(&buf, &block_count);

    while (block_count != 0) {
        if (block_count < 0) {
            block_count = block_count * -1;
            read_int64(&buf, &block_size);
        }

        if (block_size > 0) {
            buf += block_size;
        } else {
            for (i = 0; i < (size_t)block_count; i++, index++) {
                int64_t type_index;
                read_int64(&buf, &type_index);
                const avro_type_t curr_type = types[type_index];
                switch (curr_type) {
                    case AVRO_STRING:
                        int64_t str_length;
                        read_int64(&buf, &str_length);
                        buf += str_length;
                        break;
                    case AVRO_DOUBLE:
                        buf += 8;
                        break;
                    case AVRO_INT64:
                        int64_t dummy;
                        read_int64(&buf, &dummy);
                        // printf("%ld\n", dummy);
                        break;
                    case AVRO_NULL:
                    default:;
                        // do nothing
                }
            }
        }
        read_int64(&buf, &block_count);
    }
    *outer_buf = buf;
}

void skip_array_long(char **outer_buf) {
    char *buf = *outer_buf;
    size_t i;         /* index within the current block */
    size_t index = 0; /* index within the entire array */
    int64_t block_count;
    int64_t block_size = -1;

    read_int64(&buf, &block_count);

    while (block_count != 0) {
        if (block_count < 0) {
            block_count = block_count * -1;
            read_int64(&buf, &block_size);
        }

        if (block_size > 0) {
            buf += block_size;
        } else {
            for (i = 0; i < (size_t)block_count; i++, index++) {
                int64_t dummy;
                read_int64(&buf, &dummy);
                // printf("%ld\n", dummy);
            }
        }
        read_int64(&buf, &block_count);
    }
    *outer_buf = buf;
}

void skip_array_double(char **outer_buf) {
    char *buf = *outer_buf;
    size_t i;         /* index within the current block */
    size_t index = 0; /* index within the entire array */
    int64_t block_count;
    int64_t block_size = -1;

    read_int64(&buf, &block_count);

    while (block_count != 0) {
        if (block_count < 0) {
            block_count = block_count * -1;
            read_int64(&buf, &block_size);
        }

        if (block_size > 0) {
            buf += block_size;
        } else {
            for (i = 0; i < (size_t)block_count; i++, index++) {
                buf += 8;
            }
        }
        read_int64(&buf, &block_count);
    }
    *outer_buf = buf;
}

void skip_array_string(char **outer_buf) {
    char *buf = *outer_buf;
    size_t i;         /* index within the current block */
    size_t index = 0; /* index within the entire array */
    int64_t block_count;
    int64_t block_size = -1;

    read_int64(&buf, &block_count);

    while (block_count != 0) {
        if (block_count < 0) {
            block_count = block_count * -1;
            read_int64(&buf, &block_size);
        }

        if (block_size > 0) {
            buf += block_size;
        } else {
            for (i = 0; i < (size_t)block_count; i++, index++) {
                int64_t str_length;
                read_int64(&buf, &str_length);
                buf += str_length;
            }
        }
        read_int64(&buf, &block_count);
    }
    *outer_buf = buf;
}

typedef struct avro_iterator {
    int64_t num_records;
    int64_t num_bytes;
    char *prev_header;
    char *ptr;
    char *eof;
    int curr_record_index;
} avro_iterator_t;

typedef struct schema_elem {
    avro_type_t types[2];  // hardcode it to be up to 2 for now
    uint32_t num_types;
    schema_elem *children;
    int num_children;
} schema_elem_t;

typedef struct avro_context {
    avro_iterator_t *itr;
    const schema_elem_t *schema;
    int num_schema_elems;
    int query_field_index;
    const char *query_str;
} avro_context_t;

void read_schema(const schema_elem_t *elem, char **ptr, avro_type_t *ret_type) {
    if (elem->num_types == 1) {
        *ret_type = elem->types[0];
    } else {
        int64_t type_index;
        read_int64(ptr, &type_index);
        *ret_type = elem->types[type_index];
    }
}

int single_record_contains(char **prev_ptr, avro_context_t *ctx) {
    char *ptr = *prev_ptr;
    const char *query_str = ctx->query_str;
    const size_t query_str_length = strlen(ctx->query_str);
    const int num_schema_elems = ctx->num_schema_elems;
    const schema_elem_t *schema = ctx->schema;
    const int query_field_index = ctx->query_field_index;

    int ret = 0;

    avro_type_t curr_type;
    for (int i = 0; i < num_schema_elems; ++i) {
        const schema_elem_t elem = schema[i];
        read_schema(&elem, &ptr, &curr_type);
        switch (curr_type) {
            // For all cases: if we're not yet at the field we care about
            // (`query_field_index`) then we skip over the field
            case AVRO_STRING: {
                int64_t str_length;
                read_int64(&ptr, &str_length);
                if (i == query_field_index) {
#ifdef DEBUG
                    char printbuf[str_length + 1];
                    strncpy(printbuf, ptr, str_length);
                    printbuf[str_length] = '\0';
                    printf("%d: %s\n", i, printbuf);
#endif
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
                read_int64(&ptr, &val);
                // printf("%ld\n", val);
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
            case AVRO_BOOLEAN: {
                bool val;
                read_boolean(&ptr, &val);
                // printf("%s\n", val ? "true" : "false");
                if (i == query_field_index) {
                    ret = val;
                }
                break;
            }
            case AVRO_ARRAY: {
                // read schema entry of first (and only child)
                if (elem.children[0].num_types == 2) {
                    skip_array(&ptr, elem.children[0].types);
                } else {
                    avro_type_t array_type;
                    read_schema(&elem.children[0], &ptr, &array_type);
                    // for now, we don't support looking in an array, only
                    // skipping an array
                    if (array_type == AVRO_INT64) {
                        skip_array_long(&ptr);
                    } else if (array_type == AVRO_DOUBLE) {
                        skip_array_double(&ptr);
                    } else if (array_type == AVRO_STRING) {
                        skip_array_string(&ptr);
                    }
                }
                break;
            }
            case AVRO_RECORD: {
                avro_context_t ctx_copy;
                ctx_copy.query_str = ctx->query_str;
                ctx_copy.num_schema_elems = elem.num_children;
                ctx_copy.schema = elem.children;
                if (ctx->query_field_index > elem.num_children) {
                    ctx_copy.query_field_index =
                        ctx->query_field_index % elem.num_children;
                } else {
                    ctx_copy.query_field_index = -1;
                }
                // we copy the current avro context, but change the schema
                // to
                // be the schema of the sub-record
                const int val = single_record_contains(&ptr, &ctx_copy);
                if (val > 0) {
                    // if we found what we're looking for in the sub-record,
                    // we're done
                    ret = val;
                }
                break;
            }
            case AVRO_NULL:
            default:;
                // do nothing
        }
    }
    // if, somehow, we reach this point, return false
    *prev_ptr = ptr;
    return ret;
}

/**
 * Determine if a record contains a match for `ctx->query_str`. `line` should
 * fall somewhere in between the middle of a record; we assume that
 * `ctx->itr` points to the beginning of an Avro record, according to the schema
 * defined in `ctx->schema. If we find the record that contains `line` and it
 * also matches the `query_str`, return True; else return False.
 **/
int record_contains(avro_context_t *ctx, const char *line) {
    const int num_schema_elems = ctx->num_schema_elems;
    const schema_elem_t *schema = ctx->schema;
    avro_iterator_t *itr = ctx->itr;

    // First, find the record we need to check. Traverse through each record,
    // until we pass `line`.  Once we pass it, that means the record we just
    // processed needs to checked; go back and check it.
    char *prev_record = itr->ptr;
    bool check = false;
    avro_type_t curr_type;
    while (itr->curr_record_index < itr->num_records) {
        for (int i = 0; i < num_schema_elems; ++i) {
            const schema_elem_t elem = schema[i];
            read_schema(&elem, &itr->ptr, &curr_type);
            // don't read values for now, just skip over them
            switch (curr_type) {
                case AVRO_STRING: {
                    int64_t str_length;
                    read_int64(&itr->ptr, &str_length);
                    itr->ptr += str_length;
                    break;
                }
                case AVRO_INT32:
                case AVRO_INT64: {
                    // we can't skip over ints and longs, since they're
                    // variable-length encoded
                    int64_t val;
                    read_int64(&itr->ptr, &val);
                    break;
                }
                case AVRO_BOOLEAN: {
                    itr->ptr += 1;
                    break;
                }
                case AVRO_DOUBLE: {
                    itr->ptr += 8;
                    break;
                }
                case AVRO_FLOAT: {
                    itr->ptr += 4;
                    break;
                }
                case AVRO_ARRAY: {
                    if (elem.children[0].num_types == 2) {
                        skip_array(&itr->ptr, elem.children[0].types);
                    } else {
                        // read schema entry of first (and only child)
                        avro_type_t array_type;
                        read_schema(&elem.children[0], &itr->ptr, &array_type);
                        // for now, we don't support looking in an array, only
                        // skipping an array
                        if (array_type == AVRO_INT64) {
                            skip_array_long(&itr->ptr);
                        } else if (array_type == AVRO_DOUBLE) {
                            skip_array_double(&itr->ptr);
                        } else if (array_type == AVRO_STRING) {
                            skip_array_string(&itr->ptr);
                        }
                    }
                    break;
                }
                case AVRO_RECORD: {
                    avro_context_t ctx_copy;
                    avro_iterator_t itr_copy;
                    // first we copy the iterator: we're only going
                    // to iterate over a single record (the sub-record),
                    // but we need to start where the current pointer is
                    itr_copy.num_records = 1;
                    itr_copy.curr_record_index = 0;
                    itr_copy.ptr = itr->ptr;
                    ctx_copy.itr = &itr_copy;

                    // copy everything else, but also update the schema
                    // to be the schema of the sub-record, just like in
                    // `single_record_contains`
                    ctx_copy.query_str = ctx->query_str;
                    ctx_copy.num_schema_elems = elem.num_children;
                    ctx_copy.schema = elem.children;
                    if (ctx->query_field_index > elem.num_children) {
                        ctx_copy.query_field_index =
                            ctx->query_field_index % elem.num_children;
                    } else {
                        ctx_copy.query_field_index = -1;
                    }
                    int val = record_contains(&ctx_copy, line);
                    // update the current pointer to wherever we reached
                    // in the sub-record
                    itr->ptr = itr_copy.ptr;
                    if (val > 0) {
                        // if we found what we're looking for in the
                        // sub-record, we're done
                        return val;
                    }
                    break;
                }
                case AVRO_NULL:
                default:;
                    // do nothing
            }
        }
        itr->curr_record_index++;
        if (itr->ptr > line) {
            check = true;
            break;
        }
        prev_record = itr->ptr;
    }
    if (check) {
        return single_record_contains(&prev_record, ctx);
    }
    return 0;
}

int advance_iterator(avro_iterator_t *itr) {
    itr->ptr = itr->prev_header + itr->num_bytes;
    itr->ptr += 16;  // skip over 16-byte magic string
    if (itr->ptr >= itr->eof) {
        return 0;
    }
    read_int64(&itr->ptr, &itr->num_records);
    read_int64(&itr->ptr, &itr->num_bytes);
    itr->prev_header = itr->ptr;
    itr->curr_record_index = 0;
    return 1;
}

int verify_avro(const char *line, void *thunk) {
    if (!thunk) return 0;

    avro_context_t *ctx = (avro_context_t *)thunk;
    avro_iterator_t *itr = ctx->itr;

    // Case 1: line is behind current packet, which is weird. Abort.
    intptr_t diff = (intptr_t)line - (intptr_t)itr->ptr;
    if (diff < 0) {
        fprintf(stderr, "current packet is behind line!\n");
        return 0;
    }

    // Case 2: line is ahead of current packet. Skip forward until we
    // encapsulate that packet, and then parse it.
    while (itr->prev_header + itr->num_bytes < line) {
        advance_iterator(itr);
    }

    return record_contains(ctx, line);
}

void verify_avro_loop(avro_context_t *ctx) {
    long count = 0;
    long total = 0;
#ifdef DEBUG
    printf("Begin loop\n");
#endif
    avro_iterator_t *itr = ctx->itr;

    do {
        while (itr->curr_record_index < itr->num_records) {
            if (single_record_contains(&itr->ptr, ctx)) {
                count++;
            }
            itr->curr_record_index++;
            total++;
        }
    } while (advance_iterator(itr));
    printf("%s count - %ld\n", ctx->query_str, count);
    printf("total count - %ld\n", total);
}

#endif
