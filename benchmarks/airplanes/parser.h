/**
 * Contains routines for parsing the Airplane dataset into a array of structs format.
 */

#ifndef _PARSER_H_
#define _PARSER_H_

#include <stdlib.h>
#include <stdio.h>

#include <string.h>
#include <time.h>

#define SHORTCIR

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
    char creation_date[128];
    char modification_date[128];
} aircraft_t;

/** Parse a buffer representing the airplane SFO dataset into structs. */
int parse(char *input, aircraft_t **buf);

int parse(char *input, aircraft_t **buf) {

    const size_t BUFSZ = 4096;

    char *line;
    char *token;

    size_t capacity = (2 << 15);
    size_t length = 0;
    aircraft_t *ret = (aircraft_t *)malloc(sizeof(aircraft_t) * capacity);

    line = input;

    while ((input = strchr(line, '\n')) != NULL) {

        *input = '\0';

        if (length == capacity) {
            capacity *= 2;
            ret = (aircraft_t *)realloc(ret, sizeof(aircraft_t) * capacity);
        }

        int column = 0;
        aircraft_t *a = ret + length;

        char *line_scan = line;
        token = line_scan;

        while ((line_scan = strchr(token, ',')) != NULL) {

            *line_scan = '\0';

            switch (column) {
                case AIRCRAFT_ID:
#ifdef SHORTCIR
                    a->aircraft_id = atoi(token);
#endif
                    break;
                case TAIL_NUMBER:
#ifdef SHORTCIR
                    strncpy(a->tail_number, token, sizeof(a->tail_number));
#endif
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
                case CREATION_DATE:
                    strncpy(a->creation_date, token, sizeof(a->creation_date));
                    break;
                case MOD_DATE:
                    strncpy(a->modification_date, token, sizeof(a->modification_date));
                    break;
                default:
                    break;
            }
            column++;

#ifdef SHORTCIR
            // Short circuit optimization
            if (column >= AIRLINE) break;
#endif

            *line_scan = ',';
            token = line_scan + 1;
        }

        length++;

        *input = '\n';
        line = input + 1;
    }

    *buf = ret;
    return length;
}

#endif
