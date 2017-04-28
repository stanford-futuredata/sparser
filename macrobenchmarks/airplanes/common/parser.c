#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#include <time.h>

#include "../../../common/common.h"

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

int parse(char *input, aircraft_t **buf) {

    clock_t start, end;

    start = clock();

    const size_t BUFSZ = 4096;

    char *line;
    char *token;

    size_t capacity = (2 << 15);
    size_t length = 0;
    aircraft_t *ret = (aircraft_t *)malloc(sizeof(aircraft_t) * capacity);

    while ((line = strsep(&input, "\n")) != NULL) {

        if (length == capacity) {
            capacity *= 2;
            ret = (aircraft_t *)realloc(ret, sizeof(aircraft_t) * capacity);
        }

        int column = 0;
        aircraft_t *a = ret + length;
        while ((token = strsep(&line, ",")) != NULL) {
            switch (column) {
                case AIRCRAFT_ID:
                    a->aircraft_id = atoi(token);
                    break;
                case TAIL_NUMBER:
                    strncpy(a->tail_number, token, sizeof(a->tail_number));
                    break;
                case AIRCRAFT_MODEL:
                    strncpy(a->aircraft_model, token, sizeof(a->aircraft_model));
                    break;
                case AIRLINE:
                    strncpy(a->airline, token, sizeof(a->airline));
                    break;
                case STATUS:
                    a->active = (strcmp(token, "active") == 0);
                    break;
                // Unparsed.
                case CREATION_DATE:
                case MOD_DATE:
                default:
                    break;
            }
            column++;
        }

        length++;
    }

    *buf = ret;


    end = clock();
    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("Baseline Parser: %.3f seconds\n", cpu_time_used);

    return length;
}

int load(const char *filename, aircraft_t **buf) {
    char *data = NULL;
    read_all(filename, &data);
    int length = parse(data, buf);
    free(data);
    return length;
}
