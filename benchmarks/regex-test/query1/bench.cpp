#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <string.h>
#include <regex.h>

#include <immintrin.h>

#include "common.h"
#include "sparser.h"

#define VECSZ 32

const char *input = " Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

const char *headers_list[] = {
"OPTIONS",
"GET",
"HEAD",
"POST",
"PUT",
"DELETE",
"TRACE",
"CONNECT",
"PROPFIND",
"PROPPATCH",
"MKCOL",
"COPY",
"MOVE",
"LOCK",
"UNLOCK",
"REPORT",
"CHECKOUT",
"CHECKIN",
"UNCHECKOUT",
"MKWORKSPACE",
"UPDATE",
"LABEL",
"MERGE",
"MKACTIVITY",
"ORDERPATCH",
"ACL",
"PATCH",
"SEARCH",
"BCOPY",
"BDELETE",
"BMOVE",
"BPROPFIND",
"BPROPPATCH",
"NOTIFY",
"POLL",
"SUBSCRIBE",
"UNSUBSCRIBE",
NULL,
};

const char *headers = "OPTIONS|GET|HEAD|POST|PUT|DELETE|TRACE|CONNECT|PROPFIND|PROPPATCH|MKCOL|COPY|MOVE|LOCK|UNLOCK|REPORT|CHECKOUT|CHECKIN|UNCHECKOUT|MKWORKSPACE|UPDATE|LABEL|MERGE|MKACTIVITY|ORDERPATCH|ACL|PATCH|SEARCH|BCOPY|BDELETE|BMOVE|BPROPFIND|BPROPPATCH|NOTIFY|POLL|SUBSCRIBE|UNSUBSCRIBE";

int bench_strstr() {
  int matching = 0;
  bench_timer_t s = time_start();
  for (int i = 0; i < 10000; i++) {
    int j = 0;
    while (headers_list[j]) {
      if (strstr(input, headers_list[j])) {
        matching++; 
        break;
      }
      j++;
    }
  }

  double elapsed = time_stop(s);
  double mbps = (((double)strlen(input) * 10000.0) / 1000000 * 8) / elapsed;
  printf("%d (%f seconds, %f Mbps)\n", matching, elapsed, mbps);

  return 0;
}

int bench_avx() {
  int matching = 0;
  bench_timer_t s = time_start();
  for (int i = 0; i < 10000; i++) {
    size_t len = strlen(input);
    for (int j = 0; j < len - VECSZ; j += VECSZ) {
      __m256i iv = _mm256_loadu_si256((__m256i *)(input + j));
      __m256i iv1 = _mm256_loadu_si256((__m256i *)(input + j + 1));
      int k = 0;
      while (headers_list[k]) {
        int16_t x = *((int16_t *)headers_list[k]);
        __m256i compare = _mm256_set1_epi16(x);
        if (_mm256_movemask_epi8(_mm256_cmpeq_epi16(iv, compare))) {
          matching++;
          break;
        } else if (_mm256_movemask_epi8(_mm256_cmpeq_epi16(iv1, compare))) {
          matching++;
          break;
        }
        k++;
      }
    }
  }

  double elapsed = time_stop(s);
  double mbps = (((double)strlen(input) * 10000.0) / 1000000 * 8) / elapsed;
  printf("%d (%f seconds, %f Mbps)\n", matching, elapsed, mbps);

  return 0;
}

int bench_regex() {
  regex_t regex;

  // For error reporting
  int ret;
  char msgbuf[100];

  int matching = 0;

  char *pattern;
  asprintf(&pattern, "^[[:space:]]*(%s)[[:space:]]*", headers);
  printf("%s\n", pattern);

  /* Compile regular expression */
  if (regcomp(&regex, pattern, REG_EXTENDED)) {
    fprintf(stderr, "Could not compile regex %s\n", pattern);
    free(pattern);
    return -1;
  }

  bench_timer_t s = time_start();

  for (int i = 0; i < 10000; i++) {

    /* Execute regular expression */
    ret = regexec(&regex, input, 0, NULL, 0);
    if (ret == 0) {
      matching++;
    } else if (ret == REG_NOMATCH) {
    } else {
      regerror(ret, &regex, msgbuf, sizeof(msgbuf));
      fprintf(stderr, "Regex match failed: %s\n", msgbuf);
      free(pattern);
    }
  }

  double elapsed = time_stop(s);
  double mbps = (((double)strlen(input) * 10000.0) / 1000000 * 8) / elapsed;
  printf("%d (%f seconds, %f Mbps)\n", matching, elapsed, mbps);

  return 0;
}

int main() {
  printf("Input data size: %d\n", strlen(input) * 10000);
  bench_strstr();
  bench_regex();
  bench_avx();
  return 0;
}
