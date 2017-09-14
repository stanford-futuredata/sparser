#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <string.h>

#include <arpa/inet.h>
#include <immintrin.h>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "bench_json.h"
#include "sparser.h"
#include "mison.h"


// For compression
#include <zlib.h>

using namespace rapidjson;

// The query strings.
const char *TEXT = "Donald Trump";
const char *TEXT2 = "Putin";


/* CHUNK is the size of the memory chunk used by the zlib routines. */

#define CHUNK 0x4000

/* The following macro calls a zlib routine and checks the return
   value. If the return value ("status") is not OK, it prints an error
   message and exits the program. Zlib's error statuses are all less
   than zero. */

#define CALL_ZLIB(x) {                                                  \
        int status;                                                     \
        status = x;                                                     \
        if (status < 0) {                                               \
            fprintf (stderr,                                            \
                     "%s:%d: %s returned a bad status of %d.\n",        \
                     __FILE__, __LINE__, #x, status);                   \
            exit (EXIT_FAILURE);                                        \
        }                                                               \
    }

/* if "test" is true, print an error message and halt execution. */

#define FAIL(test,message) {                             \
        if (test) {                                      \
            inflateEnd (& strm);                         \
            fprintf (stderr, "%s:%d: " message           \
                     " file '%s' failed: %s\n",          \
                     __FILE__, __LINE__, file_name,      \
                     strerror (errno));                  \
            exit (EXIT_FAILURE);                         \
        }                                                \
    }

/* These are parameters to inflateInit2. See
   http://zlib.net/manual.html for the exact meanings. */

#define windowBits 15
#define ENABLE_ZLIB_GZIP 32

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
bool rapidjson_parse(const char *line) {
  Document d;
  d.Parse(line);
  if (d.HasParseError()) {
    fprintf(stderr, "\nError(offset %u): %s\n", (unsigned)d.GetErrorOffset(),
            GetParseError_En(d.GetParseError()));
    //fprintf(stderr, "Error line: %s", line);
    return false;
  }

  Value::ConstMemberIterator itr = d.FindMember("text");
  if (itr == d.MemberEnd()) {
    // The field wasn't found.
    return false;
  }
  if (strstr(itr->value.GetString(), TEXT) == NULL) {
    return false;
  }

  if (strstr(itr->value.GetString(), TEXT2) == NULL) {
    return false;
  }

  return true;
}

void decompress_gzip() {
    const char * file_name = "test.gz";
    FILE * file;
    z_stream strm = {0};
    unsigned char in[CHUNK];
    unsigned char out[CHUNK];

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.next_in = in;
    strm.avail_in = 0;
    CALL_ZLIB (inflateInit2 (& strm, windowBits | ENABLE_ZLIB_GZIP));

    /* Open the file. */

    file = fopen (file_name, "rb");
    FAIL (! file, "open");
    while (1) {
        int bytes_read;

        bytes_read = fread (in, sizeof (char), sizeof (in), file);
        FAIL (ferror (file), "read");
        strm.avail_in = bytes_read;
        do {
	    unsigned have;
            strm.avail_out = CHUNK;
	    strm.next_out = out;
            CALL_ZLIB (inflate (& strm, Z_NO_FLUSH));
	    have = CHUNK - strm.avail_out;
	    fwrite (out, sizeof (unsigned char), have, stdout);
        }
        while (strm.avail_out == 0);
        if (feof (file)) {
            inflateEnd (& strm);
            break;
        }
    }
    FAIL (fclose (file), "close");
    return 0;
}

int main() {
  const char *filename = path_for_data("tweets.json");


  // bench_rapidjson actually works for any generic parser.
  double b = bench_rapidjson(filename, rapidjson_parse);

  free(first);
  free(second);

  bench_read(filename);

  printf("Speedup: %f\n", b / a);

  return 0;
}
