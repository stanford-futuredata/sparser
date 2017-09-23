#ifndef _GZIP_UNCOMP_
#define _GZIP_UNCOMP_

#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <errno.h>
#include <assert.h>

#include <zlib.h>

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

int uncompress_gzip(unsigned char *data, unsigned char **out_a, size_t length) {

  const unsigned INC = 10;
  unsigned char *out = (unsigned char *)malloc(CHUNK * INC);
  // capacity in chunks.
  size_t capacity = INC;
  // Offset of out to write at (i.e., number of bytes written so far).
  size_t out_offset = 0;

  z_stream strm = {0};
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;

  strm.avail_in = 0;

  CALL_ZLIB(inflateInit2(&strm, windowBits | ENABLE_ZLIB_GZIP));

  for (size_t in_offset = 0; in_offset < length; in_offset += CHUNK) {

    // read CHUNK bytes if available, or whatever is left.
    size_t readsz = (length - in_offset < CHUNK) ? length - in_offset : CHUNK;

    // If we're out of space, allocate some more.
    if (CHUNK * capacity <= out_offset) {
      out = (unsigned char *)realloc(out, CHUNK * capacity * 2);
      capacity *= 2;
    }

    // Inflation routine. Write inflated bytes.
    strm.next_in = data + in_offset;
    strm.avail_in = readsz;
    
    size_t prev = strm.total_in;

    do {

      size_t have;
      size_t space = capacity * CHUNK - out_offset;
      strm.avail_out = space;
      strm.next_out = out + out_offset;
      CALL_ZLIB(inflate(&strm, Z_NO_FLUSH));

      have = space - strm.avail_out;
      out_offset += have;

      // If we're out of space, allocate some more.
      if (CHUNK * capacity <= out_offset) {
        out = (unsigned char *)realloc(out, CHUNK * capacity * 2);
        capacity *= 2;
      }

    } while (strm.total_in - prev != readsz);

    // finished!
    if (readsz != CHUNK) {
      inflateEnd(&strm);
      break;
    }
  }

  *out_a = out;
  return out_offset;
}

#endif
