#ifndef _SPARSER_H_
#define _SPARSER_H_

#include <immintrin.h>

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>
#include <assert.h>

#include <arpa/inet.h>

#include "common.h"
#include "bitmap.h"
#include "rdtsc.h"

// Checks if bit i is set in n.
#define IS_SET(n, i) (n & (0x1L << i))

// Uncomment to show timing information for calibration phase.
#define CALIBRATE_TIMING

// Size of the register we use.
const int VECSZ = 32;
// Max size of a single search string.
const int SPARSER_MAX_QUERY_LENGTH = 4 + 1;
// Max number of search strings in a single query.
const int SPARSER_MAX_QUERY_COUNT = 32;

// Defines a sparser query.
typedef struct sparser_query_ {
  unsigned count;
  char queries[SPARSER_MAX_QUERY_COUNT][SPARSER_MAX_QUERY_LENGTH];
  size_t lens[SPARSER_MAX_QUERY_COUNT];
} sparser_query_t;

/// Takes a register containing a search token and a base address, and searches
/// the base address for the search token.
typedef int (*sparser_searchfunc_t)(__m256i, const char *);

// The callback fro the single parse function. The callback MUST be able to handle
// NULL data (i.e., it should check for NULL and  still return true or false appropriately).
typedef int (*sparser_callback_t)(const char *input, void *);

typedef struct sparser_stats_ {
  // Number of times the search query matched.
  long total_matches;
  // Number of records sparser passed.
  long sparser_passed;
  // Number of records the callback passed by returning true.
  long callback_passed;
  // Total number of bytes we had to walk forward to see a new record,
  // when a match was found.
  long bytes_seeked_forward;
  // Total number of bytes we had to walk backward to see a new record,
  // when a match was found.
  long bytes_seeked_backward;
  // Fraction that sparser passed that the callback also passed
  double fraction_passed_correct;
  // Fraction of false positives.
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
  mask &= 0x55555555;

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

sparser_query_t *sparser_new_query() {
  return (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
}

/* Adds a search term to the query. The search term is clipped at either 1, 2, or
 * 4 bytes.
 *
 * @param query the query
 * @param string the search string, clipped to 1, 2, or 4 bytes.
 *
 * @return 0 if successful, nonzero otherwise.
 */
int sparser_add_query(sparser_query_t *query, const char *string) {
  if (query->count >= SPARSER_MAX_QUERY_COUNT) {
    return -1;
  }

  // Clip to the lowest multiple of 2.
  size_t len = (strnlen(string, SPARSER_MAX_QUERY_LENGTH + 1) / 2) * 2;
  if (len != 1 && len != 2 && len != 4) {
    return 1;
  }

  strncpy(query->queries[query->count], string, len);
  query->queries[query->count][len] = '\0';

  query->lens[query->count] = len;
  query->count++;
  return 0;
}


/* Adds a binary search term to the query. The search term must be 1, 2, or 4 bytes long.
 *
 * @param query the query
 * @param string the search string, clipped to 1, 2, or 4 bytes.
 * @param length the length of the search string.
 *
 * @return 0 if successful, nonzero otherwise.
 */
int sparser_add_query_binary(sparser_query_t *query, const void *term, unsigned length) {
  if (query->count >= SPARSER_MAX_QUERY_COUNT) {
    return -1;
  }

  if (length != 1 && length != 2 && length != 4) {
    return -1;
  }

  memcpy(query->queries[query->count], term, length);
  query->lens[query->count] = length;
  query->count++;
  return 0;
}

/** Returns a search query given a sample input and a set of predicates. The returned search query
 * attempts to jointly minimize the search time and false positive rate.
 *
 * @param sample the sample to test.
 * @param length the length of the sample.
 * @param predicates a set of full predicates.
 * @param count the number of predicates to test.
 * @param callback the callback, which specifies whether a query passes.
 *
 * @return a search query, or NULL if an error occurred. The returned query should be returned with free().
 */
sparser_query_t *sparser_calibrate(char *sample, long length,
                                    const char **predicates, int count,
                                    sparser_callback_t callback) {
  // Maximum number of samples to try.
  const int MAX_SAMPLES = 64;
  // Maximum number of substrings to try.
  const int MAX_SUBSTRINGS = 64;

  // Stores false positive counts for each predicate.
  int false_positives[MAX_SUBSTRINGS];
  memset(false_positives, 0, sizeof(false_positives));

  // Stores false positive mask for each predicate.
  // Bit `i` is set if the ith false positive record was *passed* by the predicate.
  bitmap_t false_positives_mask[MAX_SUBSTRINGS];
  for (int i = 0; i < MAX_SUBSTRINGS; i++) {
    false_positives_mask[i] = bitmap_new(MAX_SAMPLES);
  }

  // Store the substring source (i.e., which predicate it is derived from).
  int substring_source[MAX_SUBSTRINGS];
  // Store the length of the substring.
  size_t substring_lengths[MAX_SUBSTRINGS];

  // Counts number of records processed thus far.
  long records = 0;

  // Used to construct and store substrings to try for each predicate. These are the possible
  // return values.
  char substr_buf[8192];
  long buf_offset = 0;
  unsigned long num_substrings = 0;
  char **predicate_substrings = (char **)malloc(sizeof(char *) * MAX_SUBSTRINGS);
  memset(predicate_substrings, 0, sizeof(char *) * MAX_SUBSTRINGS);

#ifdef CALIBRATE_TIMING
  double total_elapsed = 0.0;
  bench_timer_t start = time_start();
#endif

  // Create a buffer of all the substring combinations we want to try.
  // TODO do something smarter here, e.g., using the letter frequency to decide (?).
  fprintf(stderr, "Candidates:\n");
  for (int i = 0; i < count; i++) {
    const char *substring = predicates[i];
    size_t len = strlen(substring);
    int max_substring_length;

    // Use the biggest possible size, up to 4 bytes.
    if (len >= 4) {
      max_substring_length = 4;
    } else if (len >= 2) {
      max_substring_length = 2;
    } else if (len >= 1) {
      max_substring_length = 1;
    } else {
      fprintf(stderr, "%s: empty predicate\n", __func__);
      // TODO leak!
      return NULL;
    }
    //shifts = len - max_substring_length + 1;

    for (int k = 2; k <= max_substring_length; k++) {

      //  test only 1, 2, 4
      if (k == 3) continue;
      int substring_length = k;
      int shifts = len - substring_length + 1;

      // Add the shifts for the substrings into the predicate_substrings array.
      for (int j = 0; j < shifts; j++) {
        if (num_substrings >= MAX_SUBSTRINGS || buf_offset + substring_length >= sizeof(substr_buf)) {
          break;
        }
        strncpy(substr_buf + buf_offset, substring + j, substring_length);
        substr_buf[buf_offset + substring_length] = '\0';
        predicate_substrings[num_substrings] = substr_buf + buf_offset;
        fprintf(stderr, "%ld. %s\n", num_substrings+1, predicate_substrings[num_substrings]);
        substring_source[num_substrings] = i;
        substring_lengths[num_substrings] = substring_length;
        buf_offset += substring_length + 1;
        num_substrings++;
      }
    }
  }

#ifdef CALIBRATE_TIMING
  double candidategen_elapsed = time_stop(start);
  total_elapsed += candidategen_elapsed;
  fprintf(stderr, "%s Candidate Generation Time - %f\n", __func__, candidategen_elapsed);
  start = time_start();
#endif
 
  // The mask representing all predicates passed a record.
  const uint64_t allset = (0x1L << num_substrings) - 1L;
  unsigned long parse_cost = 0;

#ifdef CALIBRATE_TIMING
  double strstr_elapsed = 0;
  double callback_elapsed = 0;
#endif

  // Now search for each substring in up to MAX_SAMPLES records.
  char *line, *newline;
  size_t remaining_length = length;
  while (records < MAX_SAMPLES && (newline = (char *)memchr(sample, '\n', remaining_length)) != NULL) {

    // Emulates behavior of strsep, but uses memchr's faster implementation.
    *newline = '\0';
    line = sample;
    sample = newline + 1;
    remaining_length -= (sample - line);

    unsigned long found = 0x0L;
    for (int i = 0; i < num_substrings; i++) {
      char *predicate = predicate_substrings[i];
#ifdef CALIBRATE_TIMING
      bench_timer_t start1 = time_start();
#endif
      // TODO accelerate this -- it's slow!
      if (strstr(line, predicate)) {
        // Set this record to found for this substring.
        bitmap_set(&false_positives_mask[i], records);
        found |= (0x1L << i);
      }
#ifdef CALIBRATE_TIMING
      strstr_elapsed += time_stop(start1);
#endif
    }

    // If any of the predicates failed OR all passed and the callback failed, we have a false
    // positive from someone. Record the false positives.
    if (allset != found) {
#ifdef CALIBRATE_TIMING
      bench_timer_t start1 = time_start();
#endif
      unsigned long start = rdtsc();
      int passed = callback(line, NULL);
      unsigned long end = rdtsc();
#ifdef CALIBRATE_TIMING
      callback_elapsed += time_stop(start1);
#endif
      // TODO Take an average or something.
      parse_cost = end - start;
      if (!passed) {
        while (found) {
          int index = ffs(found) - 1;
          false_positives[index]++;
          found &= ~(0x1L << index);
        }
      }
    } else {
      // It was an actual match, so don't record it as a false positive.
      for (int i = 0; i < num_substrings; i++) {
        bitmap_unset(&false_positives_mask[i], records);
      }
    }

    records++;

    // Undo what our strsep emulation did so the input is not mutated.
    if (sample) {
      assert(*(sample - 1) == '\0');
      sample--;
      *sample = '\n';
      sample++;
    }
  }


#ifdef CALIBRATE_TIMING
  double sample_elapsed = time_stop(start);
  total_elapsed += sample_elapsed;
  fprintf(stderr, "%s Sample Colection Time - %f\n", __func__, sample_elapsed);
  fprintf(stderr, "\t(strstr Time - %f)\n", strstr_elapsed);
  fprintf(stderr, "\t(callback Time - %f)\n", callback_elapsed);
  fprintf(stderr, "\t(summed Time - %f)\n", strstr_elapsed + callback_elapsed);
#endif

  fprintf(stderr, "%s Averge Parse Time - %ld\n", __func__, parse_cost);

#ifdef CALIBRATE_TIMING
  start = time_start();
#endif

  // TODO how to generalize this to N filters?
  // Now, check combinations of two filters - this ANDs the masks of each of the predicates together.
  // Each 1 represents a false positive in the joint predicate, so we want to *minimize* the
  // number of 1s.
  long idx1 = -1;
  long idx2 = -1;
  double min = LONG_MAX;
  const double bitlength = (double)records;
  bitmap_t joint = bitmap_new(MAX_SAMPLES);
  for (int i = 0; i < num_substrings; i++) {
    for (int j = 0; j < num_substrings; j++) {
      // Don't compare substrings sourced from the same value. Check i == j to check single substrings.
      if (i != j && substring_source[i] == substring_source[j]) {
        //fprintf(stderr, "Skipping %s and %s\n", predicate_substrings[i], predicate_substrings[j]);
        continue;
      }

      bitmap_and(&joint, &false_positives_mask[i], &false_positives_mask[j]);
      uint64_t joint_rate = bitmap_count(&joint);

      double filter_cost = substring_lengths[i];
      if (i != j) {
        filter_cost += substring_lengths[j];
      }
      // Rough number of instructions per filter length unit.
      filter_cost *= 8.0;

      // The percentage of samples that have false positives.
      double rate = ((double)joint_rate) / bitlength;
      double cost = filter_cost + (rate * (double)parse_cost);

      //fprintf(stderr, "Joint rate of %s and %s: %llu (cost=%f ,best=%f)\n", predicate_substrings[i], predicate_substrings[j], joint_rate, cost, min);

      // Does the joint query improve performance?
      if (cost < min) {
        idx1 = i;
        idx2 = j;
        min = cost;
      }

#if DEBUG
      fprintf(stderr, "%s\t%d\n", predicate_substrings[i], false_positives[i]);
#endif
    }
  }

#ifdef CALIBRATE_TIMING
  double combine_elapsed = time_stop(start);
  fprintf(stderr, "%s Filter Combination Time - %f\n", __func__, combine_elapsed);
#endif

  bitmap_free(&joint);

  assert(idx1 >= 0 && idx2 >= 0);

  sparser_query_t *squery = (sparser_query_t *)malloc(sizeof(sparser_query_t));
  memset(squery, 0, sizeof(sparser_query_t));
  sparser_add_query(squery, predicate_substrings[idx1]);
  fprintf(stderr, "%s Added Predicate: %s\n", __func__, predicate_substrings[idx1]);
  if (idx1 != idx2) {
    sparser_add_query(squery, predicate_substrings[idx2]);
    fprintf(stderr, "%s Added Predicate: %s\n", __func__, predicate_substrings[idx2]);
  }

  for (int i = 0; i < MAX_SUBSTRINGS; i++) {
    bitmap_free(&false_positives_mask[i]);
  }
  free(predicate_substrings);

#ifdef CALIBRATE_TIMING
  fprintf(stderr, "%s Calibration Total Elapsed - %f\n", __func__, total_elapsed);
#endif

  return squery;
}

sparser_stats_t *sparser_search2(char *input, long length,
                                sparser_query_t *query,
                                sparser_callback_t callback,
                                void *callback_ctx) {
  assert(query->count == 1);
  assert(query->lens[0] >= 2);

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

    uint16_t x = *((uint16_t *)query->queries[0]);
  __m256i q1 = _mm256_set1_epi16(x);
  // Bitmask designating which filters matched.
  // Bit i is set if if the ith filter matched for the current record.
  unsigned matchmask = 0;

  char *endptr = strchr(input, '\n');
  long end;
  if (endptr) {
    end = endptr - input;
  } else {
    end = length;
  }

  for (long i = 0; i < length; i += VECSZ) {
    if (i > end) {
      char *endptr = strchr(input + i, '\n');
      if (endptr) {
        end = endptr - input;
      } else {
        end = length;
      }
      matchmask = 0;
    }

    if (!IS_SET(matchmask, 0)) {
        const char *base = input + i;
        __m256i val = _mm256_loadu_si256((__m256i const *)(base));
        unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi16(val, q1));

        __m256i val2 = _mm256_loadu_si256((__m256i const *)(base + 1));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val2, q1));

        __m256i val3 = _mm256_loadu_si256((__m256i const *)(base + 2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val3, q1));

        __m256i val4 = _mm256_loadu_si256((__m256i const *)(base + 3));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val4, q1));
        mask &= 0x55555555;

        unsigned matched = _mm_popcnt_u32(mask);
        if (matched > 0) {
        stats.total_matches += matched;
        matchmask |= 0x1;
        }
    }

    unsigned allset = ((1u << query->count) - 1u);
    // check if all the filters matched by checking if all the bits
    // necessary were set in matchmask.
    if ((matchmask & allset) == allset) {
      stats.sparser_passed++;

      // update start.
      long start = i;
      for (; start > 0 && input[start] != '\n'; start--)
        ;

      stats.bytes_seeked_backward += (i - start);

      // Pass the current line to a full parser.
      char a = input[end];
      input[end] = '\0';
      if (callback(input + start, callback_ctx)) {
        stats.callback_passed++;
      }
      input[end] = a;

      // Reset record level state.
      matchmask = 0;

      // Done with this record - move on to the next one.
      i = end + 1 - VECSZ;
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

// TODO fold this into the others...
sparser_stats_t *sparser_search4_binary(char *input, long length,
    sparser_query_t *query,
    sparser_callback_t callback,
    void *callback_ctx) {

  assert(query->count == 1);
  assert(query->lens[0] >= 4);

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

  uint32_t x = *((uint32_t *)query->queries[0]);
  __m256i q1 = _mm256_set1_epi32(x);

  for (long i = 0; i < length - VECSZ; i += VECSZ) {
    const char *base = input + i;
    __m256i val = _mm256_loadu_si256((__m256i const *)(base));
    unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(val, q1)) & 0x11111111;
    __m256i val2 = _mm256_loadu_si256((__m256i const *)(base + 1));
    mask |= (_mm256_movemask_epi8(_mm256_cmpeq_epi32(val2, q1)) & 0x22222222);
    __m256i val3 = _mm256_loadu_si256((__m256i const *)(base + 2));
    mask |= (_mm256_movemask_epi8(_mm256_cmpeq_epi32(val3, q1)) & 0x44444444);
    __m256i val4 = _mm256_loadu_si256((__m256i const *)(base + 3));
    mask |= (_mm256_movemask_epi8(_mm256_cmpeq_epi32(val4, q1)) & 0x88888888);

    // These designate positions where matches occurred.
    if (mask > 0) {
      stats.sparser_passed++;

      while(mask) {
        unsigned match_index = ffs(mask) - 1;
        // This is the site of the match.
        unsigned long offset = i + match_index;

        uint32_t addr = *((uint32_t *)(input + offset));
        // This is the IP address - it should match the input.
        struct in_addr a;
        a.s_addr = addr;

        callback(input + offset, callback_ctx);

        mask &= ~(1 << match_index);
      }
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

sparser_stats_t *sparser_search4(char *input, long length,
                                sparser_query_t *query,
                                sparser_callback_t callback,
                                void *callback_ctx) {
  assert(query->count == 1);
  assert(query->lens[0] >= 4);

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

    uint32_t x = *((uint32_t *)query->queries[0]);
  __m256i q1 = _mm256_set1_epi32(x);

  // Bitmask designating which filters matched.
  // Bit i is set if if the ith filter matched for the current record.
  unsigned matchmask = 0;

  char *endptr = strchr(input, '\n');
  long end;
  if (endptr) {
    end = endptr - input;
  } else {
    end = length;
  }

  for (long i = 0; i < length; i += VECSZ) {
    if (i > end) {
      char *endptr = strchr(input + i, '\n');
      if (endptr) {
        end = endptr - input;
      } else {
        end = length;
      }
      matchmask = 0;
    }

    if (!IS_SET(matchmask, 0)) {
        const char *base = input + i;
        __m256i val = _mm256_loadu_si256((__m256i const *)(base));
        unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(val, q1));

        __m256i val2 = _mm256_loadu_si256((__m256i const *)(base + 1));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val2, q1));

        __m256i val3 = _mm256_loadu_si256((__m256i const *)(base + 2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val3, q1));

        __m256i val4 = _mm256_loadu_si256((__m256i const *)(base + 3));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val4, q1));
        mask &= 0x11111111;

        unsigned matched = _mm_popcnt_u32(mask);
        if (matched > 0) {
        stats.total_matches += matched;
        matchmask |= 0x1;
        }
    }

    unsigned allset = ((1u << query->count) - 1u);
    // check if all the filters matched by checking if all the bits
    // necessary were set in matchmask.
    if ((matchmask & allset) == allset) {
      stats.sparser_passed++;

      // update start.
      long start = i;
      for (; start > 0 && input[start] != '\n'; start--)
        ;

      stats.bytes_seeked_backward += (i - start);

      // Pass the current line to a full parser.
      char a = input[end];
      input[end] = '\0';
      if (callback(input + start, callback_ctx)) {
        stats.callback_passed++;
      }
      input[end] = a;

      // Reset record level state.
      matchmask = 0;

      // Done with this record - move on to the next one.
      i = end + 1 - VECSZ;
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

sparser_stats_t *sparser_search2_2(char *input, long length,
                                sparser_query_t *query,
                                sparser_callback_t callback,
                                void *callback_ctx) {
  assert(query->count == 2);
  assert(query->lens[0] >= 2);
  assert(query->lens[1] >= 2);

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

  uint16_t x = *((uint16_t *)query->queries[0]);
  __m256i q1 = _mm256_set1_epi16(x);

  x = *((uint16_t *)query->queries[1]);
  __m256i q2 = _mm256_set1_epi16(x);

  // Bitmask designating which filters matched.
  // Bit i is set if if the ith filter matched for the current record.
  unsigned matchmask = 0;

  char *endptr = strchr(input, '\n');
  long end;
  if (endptr) {
    end = endptr - input;
  } else {
    end = length;
  }

  for (long i = 0; i < length; i += VECSZ) {
    if (i > end) {
      char *endptr = strchr(input + i, '\n');
      if (endptr) {
        end = endptr - input;
      } else {
        end = length;
      }
      matchmask = 0;
    }

    if (!IS_SET(matchmask, 0) || !IS_SET(matchmask, 1)) {
      const char *base = input + i;
      __m256i val = _mm256_loadu_si256((__m256i const *)(base));
      unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi16(val, q1));

      __m256i val2 = _mm256_loadu_si256((__m256i const *)(base + 1));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val2, q1));

      __m256i val3 = _mm256_loadu_si256((__m256i const *)(base + 2));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val3, q1));

      __m256i val4 = _mm256_loadu_si256((__m256i const *)(base + 3));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val4, q1));
      mask &= 0x55555555;

      unsigned matched = _mm_popcnt_u32(mask);
      if (matched > 0) {
        stats.total_matches += matched;
        matchmask |= 0x1;
      }

      if (!IS_SET(matchmask, 1)) {
        mask = _mm256_movemask_epi8(_mm256_cmpeq_epi16(val, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val2, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val3, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val4, q2));
        mask &= 0x55555555;

        matched = _mm_popcnt_u32(mask);
        if (matched > 0) {
          stats.total_matches += matched;
          matchmask |= (0x1 << 1);
        }
      }
    }

    unsigned allset = ((1u << query->count) - 1u);
    // check if all the filters matched by checking if all the bits
    // necessary were set in matchmask.
    if ((matchmask & allset) == allset) {
      stats.sparser_passed++;

      // update start.
      long start = i;
      for (; start > 0 && input[start] != '\n'; start--)
        ;

      stats.bytes_seeked_backward += (i - start);

      // Pass the current line to a full parser.
      char a = input[end];
      input[end] = '\0';
      if (callback(input + start, callback_ctx)) {
        stats.callback_passed++;
      }
      input[end] = a;

      // Reset record level state.
      matchmask = 0;

      // Done with this record - move on to the next one.
      i = end + 1 - VECSZ;
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

sparser_stats_t *sparser_search2_4(char *input, long length,
                                sparser_query_t *query,
                                sparser_callback_t callback,
                                void * callback_ctx) {
  assert(query->count == 2);
  assert((query->lens[0] >= 2 && query->lens[1] >= 4) || (query->lens[1] >= 4 && query->lens[0] >= 2));

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

  uint16_t x;
  uint32_t y;
  __m256 q1, q2;

  if (query->lens[0] >= 2 && query->lens[1] >= 4) {
    x = *((uint16_t *)query->queries[0]);
    y = *((uint32_t *)query->queries[1]);
    q1 = _mm256_set1_epi16(x);
    q2 = _mm256_set1_epi32(y);
  } else {
    x = *((uint16_t *)query->queries[1]);
    y = *((uint32_t *)query->queries[0]);
    q1 = _mm256_set1_epi16(x);
    q2 = _mm256_set1_epi32(y);
  }

  // Bitmask designating which filters matched.
  // Bit i is set if if the ith filter matched for the current record.
  unsigned matchmask = 0;

  char *endptr = strchr(input, '\n');
  long end;
  if (endptr) {
    end = endptr - input;
  } else {
    end = length;
  }

  for (long i = 0; i < length; i += VECSZ) {
    if (i > end) {
      char *endptr = strchr(input + i, '\n');
      if (endptr) {
        end = endptr - input;
      } else {
        end = length;
      }
      matchmask = 0;
    }

    if (!IS_SET(matchmask, 0) || !IS_SET(matchmask, 1)) {
      const char *base = input + i;
      __m256i val = _mm256_loadu_si256((__m256i const *)(base));
      unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi16(val, q1));

      __m256i val2 = _mm256_loadu_si256((__m256i const *)(base + 1));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val2, q1));

      __m256i val3 = _mm256_loadu_si256((__m256i const *)(base + 2));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val3, q1));

      __m256i val4 = _mm256_loadu_si256((__m256i const *)(base + 3));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi16(val4, q1));
      mask &= 0x55555555;

      unsigned matched = _mm_popcnt_u32(mask);
      if (matched > 0) {
        stats.total_matches += matched;
        matchmask |= 0x1;
      }

      if (!IS_SET(matchmask, 1)) {
        mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(val, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val2, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val3, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val4, q2));
        mask &= 0x11111111;

        matched = _mm_popcnt_u32(mask);
        if (matched > 0) {
          stats.total_matches += matched;
          matchmask |= (0x1 << 1);
        }
      }
    }

    unsigned allset = ((1u << query->count) - 1u);
    // check if all the filters matched by checking if all the bits
    // necessary were set in matchmask.
    if ((matchmask & allset) == allset) {
      stats.sparser_passed++;

      // update start.
      long start = i;
      for (; start > 0 && input[start] != '\n'; start--)
        ;

      stats.bytes_seeked_backward += (i - start);

      // Pass the current line to a full parser.
      char a = input[end];
      input[end] = '\0';
      if (callback(input + start, callback_ctx)) {
        stats.callback_passed++;
      }
      input[end] = a;

      // Reset record level state.
      matchmask = 0;

      // Done with this record - move on to the next one.
      i = end + 1 - VECSZ;
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


sparser_stats_t *sparser_search4_4(char *input, long length,
                                sparser_query_t *query,
                                sparser_callback_t callback,
                                void *callback_ctx) {
  assert(query->count == 2);
  assert(query->lens[0] >= 4);
  assert(query->lens[1] >= 4);

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

  uint32_t x = *((uint32_t *)query->queries[0]);
  __m256i q1 = _mm256_set1_epi32(x);

  x = *((uint32_t *)query->queries[1]);
  __m256i q2 = _mm256_set1_epi32(x);

  // Bitmask designating which filters matched.
  // Bit i is set if if the ith filter matched for the current record.
  unsigned matchmask = 0;

  char *endptr = strchr(input, '\n');
  long end;
  if (endptr) {
    end = endptr - input;
  } else {
    end = length;
  }

  const unsigned allset = ((1u << query->count) - 1u);

#ifdef MEASURE_CYCLES
  long measure_count = 0;
  long measure_sum = 0;
#endif

  for (long i = 0; i < length; i += VECSZ) {
    if (i > end) {
      char *endptr = strchr(input + i, '\n');
      if (endptr) {
        end = endptr - input;
      } else {
        end = length;
      }
      matchmask = 0;
    }

#ifdef MEASURE_CYCLES
    long start = rdtsc();
#endif

    if (matchmask != allset) {
      const char *base = input + i;
      __m256i val = _mm256_loadu_si256((__m256i const *)(base));
      unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(val, q1));
      //__m256 vmask =_mm256_cmpeq_epi32(val, q1);

      __m256i val2 = _mm256_loadu_si256((__m256i const *)(base + 1));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val2, q1));
      //vmask = _mm256_or_si256(vmask, _mm256_cmpeq_epi32(val2, q1));

      __m256i val3 = _mm256_loadu_si256((__m256i const *)(base + 2));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val3, q1));
      //vmask = _mm256_or_si256(vmask, _mm256_cmpeq_epi32(val3, q1));

      __m256i val4 = _mm256_loadu_si256((__m256i const *)(base + 3));
      mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val4, q1));
      //vmask = _mm256_or_si256(vmask, _mm256_cmpeq_epi32(val4, q1));

      //unsigned mask = _mm256_movemask_epi8(vmask);
      mask &= 0x11111111;

      if (mask > 0) {
        unsigned matched = _mm_popcnt_u32(mask);
        stats.total_matches += matched;
        matchmask |= 0x1;
      }

      if (!IS_SET(matchmask, 1)) {


        mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(val, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val2, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val3, q2));
        mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi32(val4, q2));
        /*
        vmask = _mm256_cmpeq_epi32(val, q2);
        vmask = _mm256_or_si256(vmask, _mm256_cmpeq_epi32(val2, q2));
        vmask = _mm256_or_si256(vmask, _mm256_cmpeq_epi32(val3, q2));
        vmask = _mm256_or_si256(vmask, _mm256_cmpeq_epi32(val4, q2));
        mask = _mm256_movemask_epi8(vmask);
        */
        mask &= 0x11111111;

        if (mask) {
          unsigned matched = _mm_popcnt_u32(mask);
          stats.total_matches += matched;
          matchmask |= (0x1 << 1);
        }
      }
    }

    // check if all the filters matched by checking if all the bits
    // necessary were set in matchmask.
    if (matchmask == allset) {
      stats.sparser_passed++;

      // update start. Using vectors here seems to only make a marginal performance difference.
      long start = i;
      __m256i nl = _mm256_set1_epi8('\n');
      for (; start >= 32; start -= 32) {
        __m256i tmp = _mm256_loadu_si256((__m256i *)(input + start - 32));
        if (_mm256_movemask_epi8(_mm256_cmpeq_epi8(tmp, nl))) {
          start -= 32;
          break;
        }
      }
      while (input[start] != '\n') start++;
      assert(input[start] == '\n');

      stats.bytes_seeked_backward += (i - start);

      // Pass the current line to a full parser.
      char a = input[end];
      input[end] = '\0';
      if (callback(input + start, callback_ctx)) {
        stats.callback_passed++;
      }
      input[end] = a;

      // Reset record level state.
      matchmask = 0;

      // Done with this record - move on to the next one.
      i = end + 1 - VECSZ;
    }

#ifdef MEASURE_CYCLES
    long end = rdtsc();
    measure_sum += end - start;
    measure_count++;
    if (measure_count > 1000000) {
      fprintf(stderr, "%s: %ld cycles/iter\n", __func__, measure_sum / measure_count);
      measure_count = 0;
      measure_sum = 0;
    }
#endif
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

/* Performs the sparser search given a compiled search query and a buffer.
 *
 * This performs a simple sparser search given the query and input buffer. It
 * only searches for one occurance of the query string in each record. Records
 * are assumed to be delimited by newline.
 *
 * @param input the buffer to search
 * @param length the size of the input buffer, in bytes.
 * @param query the query to look for
 * @param callback the callback if sparser passes the query.
 *
 * @return statistics about the run.
 * */
sparser_stats_t *sparser_search(char *input, long length,
                                sparser_query_t *query,
                                sparser_callback_t callback,
                                void *callback_ctx) {

  // Call into specializations if possible - it's much faster.
  if (query->count == 1 && query->lens[0] == 4) {
    fprintf(stderr, "Calling specialization 4\n");
    return sparser_search4(input, length, query, callback, callback_ctx);
  } else if (query->count == 2 && query->lens[0] == 4 && query->lens[1] == 4) {
    fprintf(stderr, "Calling specialization 4_4\n");
    return sparser_search4_4(input, length, query, callback, callback_ctx);
  } else if (query->count == 2 && query->lens[0] == 2 && query->lens[1] == 2) {
    fprintf(stderr, "Calling specialization 2_2\n");
    return sparser_search2_2(input, length, query, callback, callback_ctx);
  } else if (query->count == 1 && query->lens[0] == 2) {
    fprintf(stderr, "Calling specialization 2\n");
    return sparser_search2(input, length, query, callback, callback_ctx);
  } else if (query->count == 2 &&
      ((query->lens[0] == 2 && query->lens[1] == 4) ||
       (query->lens[1] == 2 && query->lens[0] == 4))) {
    fprintf(stderr, "Calling specialization 2_4\n");
    return sparser_search2_4(input, length, query, callback, callback_ctx);
  }

  fprintf(stderr, "WARN: Calling general VFC-based runtime\n");

  sparser_searchfunc_t searchfuncs[SPARSER_MAX_QUERY_COUNT];
  __m256i reg[SPARSER_MAX_QUERY_COUNT];

  sparser_stats_t stats;
  memset(&stats, 0, sizeof(stats));

  for (int i = 0; i < query->count; i++) {
    char *string = query->queries[i];
    printf("Set string %s (index=%d, len=%zu)\n", string, i, query->lens[i]);
    switch (query->lens[i]) {
    case 1: {
      searchfuncs[i] = search_epi8;
      uint8_t x = *((uint8_t *)string);
      reg[i] = _mm256_set1_epi8(x);
      break;
    }
    case 2: {
      searchfuncs[i] = search_epi16;
      uint16_t x = *((uint16_t *)string);
      reg[i] = _mm256_set1_epi16(x);
      break;
    }
    case 4: {
      searchfuncs[i] = search_epi32;
      uint32_t x = *((uint32_t *)string);
      reg[i] = _mm256_set1_epi32(x);
      break;
    }
    default: { return NULL; }
    }
  }

  // Bitmask designating which filters matched.
  // Bit i is set if if the ith filter matched for the current record.
  unsigned matchmask = 0;

  char *endptr = strchr(input, '\n');
  long end;
  if (endptr) {
    end = endptr - input;
  } else {
    end = length;
  }

  for (long i = 0; i < length; i += VECSZ) {

    if (i > end) {
      char *endptr = strchr(input + i, '\n');
      if (endptr) {
        end = endptr - input;
      } else {
        end = length;
      }
      matchmask = 0;
    }

    // Check each query.
    for (int j = 0; j < query->count; j++) {
      // Found this already.
      if (IS_SET(matchmask, j)) {
        continue;
      }

      __m256i comparator = reg[j];
      int shifts = query->lens[j];
      sparser_searchfunc_t f = searchfuncs[j];

      for (int k = 0; k < shifts; k++) {
        // Returns the number of matches.
        int matched = f(comparator, input + i + k);
        if (matched > 0) {
          stats.total_matches += matched;
          // record that this query matched.
          matchmask |= (1 << j);
          // no need to check remaining shifts.

          // Debug
          /*
          char a = input[i + k + VECSZ];
          input[i + k + VECSZ] = '\0';
          printf("%s in %s\n", query->queries[j], input + i + k);
          input[i + k + VECSZ] = a;
          */
          break;
        }
      }
    }

    unsigned allset = ((1u << query->count) - 1u);
    // check if all the filters matched by checking if all the bits
    // necessary were set in matchmask.
    if ((matchmask & allset) == allset) {
      stats.sparser_passed++;

      // update start.
      long start = i;
      for (; start > 0 && input[start] != '\n'; start--)
        ;

      stats.bytes_seeked_backward += (i - start);

      // Pass the current line to a full parser.
      char a = input[end];
      input[end] = '\0';
      if (callback(input + start, callback_ctx)) {
        stats.callback_passed++;
      }
      input[end] = a;

      // Reset record level state.
      matchmask = 0;

      // Done with this record - move on to the next one.
      i = end + 1 - VECSZ;
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

  snprintf(buf, sizeof(buf), "Distinct Query matches: %ld\n\
Sparser Passed Records: %ld\n\
Callback Passed Records: %ld\n\
Bytes Seeked Forward: %ld\n\
Bytes Seeked Backward: %ld\n\
Fraction Passed Correctly: %f\n\
Fraction False Positives: %f",
           stats->total_matches, stats->sparser_passed, stats->callback_passed,
           stats->bytes_seeked_forward, stats->bytes_seeked_backward,
           stats->fraction_passed_correct, stats->fraction_passed_incorrect);
  return buf;
}

#endif


