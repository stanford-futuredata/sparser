
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <time.h>

struct data {
    // The line buffer
    char *_buf;

    char *city;
    char *state_short;
    char *state_full;
    char *county;
    char *city_alias;
};

// Represents a generic CSV parsing routine.
int parse_data(const char *filename, const char *delim, struct data **output) {
    const unsigned SIZE = 4096;
    static char buf[SIZE];

    unsigned capacity = 16;
    unsigned length = 0;
    struct data *data = (struct data *)malloc(sizeof(struct data) * capacity);

    FILE *f = fopen(filename, "r");
    assert(f);

    while (fgets(buf, SIZE, f)) {
        if (length >= capacity) {
            capacity *= 2;
            data = (struct data *)realloc(data, sizeof(struct data) * capacity);
        }

        unsigned slen = strnlen(buf, SIZE) + 1;
        data[length]._buf = malloc(sizeof(char) * slen);
        strncpy(data[length]._buf, buf, slen);

        // Parse out each field.
        char *s = data[length]._buf;
        data[length].city = strsep(&s, delim);
        data[length].state_short = strsep(&s, delim);
        data[length].state_full = strsep(&s, delim);
        data[length].county = strsep(&s, delim);
        data[length].city_alias = strsep(&s, delim);
        assert(!s);

        length++;
    }

    fclose(f);
    *output = data;

    return length;
}

int process_inline(const char *filename, const char *delim) {
    const unsigned SIZE = 4096;
    static char buf[SIZE];

    FILE *f = fopen(filename, "r");
    assert(f);

    int result = 0;

    while (fgets(buf, SIZE, f)) {
        char *s = buf;
        char *ret_val = strsep(&s, delim); // city
        ret_val = strsep(&s, delim); // state_short
        result += strlen(ret_val);
        ret_val = strsep(&s, delim); // state_full
        result += strlen(ret_val);
        //ret_val = strsep(&s, delim); // county
        //result += strlen(ret_val);
        //ret_val = strsep(&s, delim); // city_alias
        //result += strlen(ret_val);
    }

    fclose(f);
    return result;
}

int main() {
    const char *filename = "data.csv";
    time_t start, end;

    start = clock();
    struct data *data;
    int records = parse_data(filename, "|", &data);
    int lens = 0;
    for (int i = 0; i < records; i++) {
        lens += strlen(data[i].state_short);
        lens += strlen(data[i].state_full);
        //lens += strlen(data[i].county);
        //lens += strlen(data[i].city_alias);
    }
    end = clock();

    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%d (%.3f seconds)\n", lens, cpu_time_used);

    start = clock();
    lens = process_inline(filename, "|");
    end = clock();

    double cpu_time_used_2 = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%d (%.3f seconds)\n", lens, cpu_time_used_2);

    printf("%.3fx speedup with inlining\n", cpu_time_used / cpu_time_used_2);
}
