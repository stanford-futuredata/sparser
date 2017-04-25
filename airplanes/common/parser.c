#include <stdlib.h>
#include <stdio.h>

#include <string.h>

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

char *read_all(const char *filename) {
    FILE *f = fopen(filename, "r");

	fseek(f, 0, SEEK_END);
	long fsize = ftell(f);
	fseek(f, 0, SEEK_SET);  //same as rewind(f);

	char *string = malloc(fsize + 1);
	fread(string, fsize, 1, f);
	fclose(f);

	string[fsize] = 0;
	return string;
}

int parse(char *input, aircraft_t **buf) {
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
    return length;
}

int load(const char *filename, aircraft_t **buf) {
    char *data = read_all(filename);
    int length = parse(data, buf);
    free(data);
    return length;
}
