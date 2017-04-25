#ifndef _PARSER_H_
#define _PARSER_H_

typedef enum {
    AIRCRAFT_ID = 0,
    TAIL_NUMBER,
    AIRCRAFT_MODEL,
    AIRLINE,
    STATUS,
    CREATION_DATE,
    MOD_DATE,
} aircraft_columns_t;

typedef struct {
    int aircraft_id;
    char tail_number[128];
    char aircraft_model[128];
    char airline[128];
    int active;

    // Unparsed.
    int creation_date;
    int modification_date;
} aircraft_t;

char *read_all(const char *filename);
int *parse(char *input, aircraft_t **buf);
int load(const char *filename, aircraft_t **buf);

#define benchmark(ptr, STATEMENT) do { \
    time_t start, end; \
    start = clock(); \
    ptr = (void *)&arg; \
    end = clock(); \
    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC; \
    printf("%ld (%.3f seconds)\n", result, cpu_time_used); \
} while(0);

#endif
