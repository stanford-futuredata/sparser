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
