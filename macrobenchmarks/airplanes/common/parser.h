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

int *parse(char *input, aircraft_t **buf);
int load(const char *filename, aircraft_t **buf);

#endif
