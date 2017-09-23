#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <errno.h>
#include <assert.h>

#include <zlib.h>

#include <common.h>
#include "gzip_uncompress.h"

int main (int argc, char **argv) {

    if (argc != 2){
      fprintf(stderr, "give me a filename, bruh\n");
      exit(1);
    }

    const char *filename = argv[1];

    char *data;
    size_t length = read_all(filename, &data) - 1;

    unsigned char *uncompressed;
    size_t size = uncompress_gzip((unsigned char *)data, &uncompressed, length);

    FILE *f = fopen("out.json", "w");
    fwrite(uncompressed, sizeof(unsigned char), size, f);
    fclose(f);
}
