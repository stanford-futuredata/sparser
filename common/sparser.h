#ifndef _SPARSER_H_
#define _SPARSER_H_

#include <immintrin.h>

#include <stdio.h>
#include <string.h>

// Size of the register we use.
const int VECSZ = 32;

/// Takes a register containing a search token and a base address, and searches
/// the base address for the search token.
typedef int (*sparser_searchfunc_t)(__m256i, const char *);

// The callback fro the single parse function.
typedef bool (*sparser_callback_t)(const char *input);

typedef struct sparser_stats_ {
  long total_matches;
  long sparser_passed;
  long callback_passed;
  long bytes_seeked_forward;
  long bytes_seeked_backward;
  double fraction_passed_correct;
  double fraction_passed_incorrect;
} sparser_stats_t;

/** Search for an 8-bit search string.
 *
 * @param reg the register filled with the search value
 * @param base the data to search. Should be at least 32 bytes long.
 *
 * @return the number of matches found.
 */
int search_epi8(__m256i reg, const char *base) {
  int count = 0;
  __m256i val = _mm256_loadu_si256((__m256i const *)(base));
  unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(reg, val));

  // TODO popcnt better here, since we're not returning positions?
  while (mask) {
    int index = ffs(mask) - 1;
    mask &= ~(1 << index);
    count++;
  }
  return count;
}

/** Search for an 16-bit search string.
 *
 * @param reg the register filled with the search value
 * @param base the data to search. Should be at least 32 bytes long.
 *
 * @return the number of matches found.
 */
int search_epi16(__m256i reg, const char *base) {
  int count = 0;
  __m256i val = _mm256_loadu_si256((__m256i const *)(base));
  unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi16(reg, val));
  mask &= 0xf0f0f0f0f0;

  while (mask) {
    int index = ffs(mask) - 1;
    mask &= ~(1 << index);
    count++;
  }
  return count;
}

/** Search for an 32-bit search string.
 *
 * @param reg the register filled with the search value
 * @param base the data to search. Should be at least 32 bytes long.
 *
 * @return the number of matches found.
 */
int search_epi32(__m256i reg, const char *base) {
  int count = 0;
  __m256i val = _mm256_loadu_si256((__m256i const *)(base));
  unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(reg, val));
  mask &= 0x11111111;

  while (mask) {
    int index = ffs(mask) - 1;
    mask &= ~(1 << index);
    count++;
  }
  return count;
}

/* Performs the sparser search given a single search query and a buffer.
 *
 * This performs a simple sparser search given the query and input buffer. It
 * only searches for one occurance of the query string in each record. Records
 * are assumed to be delimited by newline.
 *
 * @param input the buffer to search
 * @param query the query to look for
 * @param length the size of the input buffer, in bytes.
 * @param query_length the size of the query string, in bytes.
 *
 * @return statistics about the run.
 * */
sparser_stats_t *sparser_search_single(char *input, long length,
                                       const char *query, long qlength,
                                       sparser_callback_t callback) {

  sparser_searchfunc_t searchfunc;
  __m256i reg;

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

  switch (qlength) {
  case 1: {
    searchfunc = search_epi8;
    uint8_t x = *((uint8_t *)query);
    reg = _mm256_set1_epi8(x);
    break;
  }
  case 2: {
    searchfunc = search_epi16;
    uint16_t x = *((uint16_t *)query);
    reg = _mm256_set1_epi16(x);
    break;
  }
  case 4: {
    searchfunc = search_epi32;
    uint32_t x = *((uint32_t *)query);
    reg = _mm256_set1_epi32(x);
    break;
  }
  default: { return NULL; }
  }

  int shifts = qlength;
  for (long i = 0; i < length - VECSZ - shifts; i += VECSZ) {

    long matches = 0;
    for (int j = 0; j < shifts; j++) {
      matches += searchfunc(reg, input + i + j);
    }

    stats.total_matches += matches;

    // Found something! Seek back to the beginning of the token and invoke the
    // callback..
    if (matches > 0) {

      stats.sparser_passed++;

      long record_end = i;
      for (; record_end < length && input[record_end] != '\n'; record_end++)
        ;
      input[record_end] = '\0';

      stats.bytes_seeked_forward += (record_end - i);

      // Seek back to the previous newline so we can pass the full record to the
      // parser.
      long record_start = i;
      for (; record_start > 0 && input[record_start] != '\0' &&
             input[record_start] != '\n';
           record_start--)
        ;

      if (record_start != 0) {
        input[record_start] = '\0';
        record_start++;
      }

      stats.bytes_seeked_backward += (i - record_start);

      if (callback(input + record_start)) {
        stats.callback_passed++;
        // TODO do something here.
      }
      i = record_end + 1 - VECSZ;
    }
  }

  if (stats.sparser_passed > 0) {
    stats.fraction_passed_correct =
        (double)stats.callback_passed / (double)stats.sparser_passed;
    stats.fraction_passed_incorrect = 1.0 - stats.fraction_passed_correct;
  }

  sparser_stats_t *ret = (sparser_stats_t *)malloc(sizeof(sparser_stats_t));
  memcpy(ret, &stats, sizeof(stats));

  return ret;
}

static char *sparser_format_stats(sparser_stats_t *stats) {
  static char buf[8192];

  snprintf(buf, sizeof(buf), "Query matches: %ld\n\
Sparser Passed Records: %ld\n\
Callback PassedRecords: %ld\n\
Bytes Seeked Forward: %ld\n\
Bytes Seeked Backward: %ld\n\
Fraction Passed Correctly: %f\n\
Fraction False Positives: %f", stats->total_matches,
           stats->sparser_passed, stats->callback_passed,
           stats->bytes_seeked_forward, stats->bytes_seeked_backward,
           stats->fraction_passed_correct, stats->fraction_passed_incorrect);
  return buf;
}

#endif
