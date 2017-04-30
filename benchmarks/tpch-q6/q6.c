#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include <immintrin.h>

#include "common.h"

// Columnar TPCH Q6 data.
struct lineitem_q6 {
    int *l_shipdate;
    float *l_discount;
    float *l_quantity;
    float *l_extendedprice;
    size_t length;
};

typedef enum {
    L_ORDERKEY = 0,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT,
} lineitem_keys_t;

/** Parse a date formated as YYYY-MM-DD into an integer, maintaining
 * sort order.
 *
 * @param date a properly formatted date string.
 *
 * @return an integer representing the date. The integer maintains
 * ordering (i.e. earlier dates have smaller values).
 * Returns -1 if the parse failed.
 *
 */
int parse_date(const char *date) {
    char *buf, *tofree;
    tofree = buf = strdup(date);

    char *c;
    char *token;
    long value;
    long result[3];

    int i = 0;
    while ((token = strsep((char **)(&buf), "-")) != NULL) {
        value = strtoul(token, &c, 10);
        if (token == c) {
            return -1;
        }

        result[i] = value;
        i++;
        if (i > 3)
            break;
    }

    free(tofree);
    return result[2] + result[1] * 100 + result[0] * 10000;
}

struct lineitem_q6 *load_q6(FILE *tbl, int sf) {

    const int LINES_PER_SF = 6000000;

    const int SIZE = 4096;
    char buf[SIZE];

    int i = 0;
    int ret = -1;

    struct lineitem_q6 *items = NULL;
    int capacity;

    if (!tbl) {
        perror("couldn't open data file");
        goto fail;
    }

    items = (struct lineitem_q6 *)malloc(sizeof(struct lineitem_q6));
    capacity = sf > 0 ? LINES_PER_SF * sf : LINES_PER_SF;

    items->l_shipdate = (int *)malloc(sizeof(int) * capacity);
    items->l_discount = (float *)malloc(sizeof(float) * capacity);
    items->l_quantity = (float *)malloc(sizeof(float) * capacity);
    items->l_extendedprice = (float *)malloc(sizeof(float) * capacity);

    while (fgets(buf, SIZE, tbl)) {
        char *line = buf;
        char *token;
        int column = 0;
        char *c;

        if (i >= capacity) {
            capacity += LINES_PER_SF;
            items->l_shipdate = (int *)realloc(items->l_shipdate, sizeof(int) * capacity);
            items->l_discount = (float *)realloc(items->l_discount, sizeof(float) * capacity);
            items->l_quantity = (float *)realloc(items->l_quantity, sizeof(float) * capacity);
            items->l_extendedprice = (float *)realloc(items->l_extendedprice, sizeof(float) * capacity);
        }

        // We'll do the predicate pushdown here for free, since it's columnar data anyways.
        while ((token = strsep(&line, "|")) != NULL) {
            double value;
            switch (column) {
                case L_SHIPDATE:
                    items->l_shipdate[i] = parse_date(token);
                    break;
                case L_QUANTITY:
                    value = strtod(line, &c);
                    assert(c != line);
                    items->l_quantity[i] = value;
                    break;
                case L_DISCOUNT:
                    value = strtod(line, &c);
                    assert(c != line);
                    items->l_discount[i] = value;
                    break;
                case L_EXTENDEDPRICE:
                    value = strtod(line, &c);
                    assert(c != line);
                    items->l_extendedprice[i] = value;
                    break;
                default:
                    break;
            }
            column++;
        }
        i++;
    }

    items->length = i;

exit:
    return items;
fail:
    if (items) {
        free(items->l_shipdate);
        free(items->l_discount);
        free(items->l_quantity);
        free(items->l_extendedprice);
        free(items);
    }
    return NULL;
}

int q6(struct lineitem_q6 *items) {

  size_t i;

  int *l_shipdate = items->l_shipdate;
  float *l_discount = items->l_discount;
  float *l_quantity = items->l_quantity;
  float *l_extendedprice = items->l_extendedprice;
  size_t length = items->length;

  // The vectors used for comparison.
  // We add (or subtract) the 1 since we're using a > rather than >= instruction
  const __m256i v_shipdate_lower = _mm256_set1_epi32(19940101 - 1);
  const __m256i v_shipdate_upper = _mm256_set1_epi32(19950101);

  const __m256 v_discount_lower = _mm256_set1_ps(0.06 - 1);
  const __m256 v_discount_upper = _mm256_set1_ps(0.06 + 1);
  const __m256 v_quantity_upper = _mm256_set1_ps(24.0);

  __m256i v_sum = _mm256_setzero_ps();
  // For aggregating the sum.
  __m128i v_high, v_low;

  float result = 0.0;
  for (i = 0; i+8 <= length; i += 8) {
    __m256i v_shipdate;
    __m256 v_discount, v_quantity, v_extendedprice;
    __m256 v_p0, v_result;

    v_shipdate = _mm256_lddqu_si256((const __m256i *)(l_shipdate + i));
    v_discount = _mm256_loadu_ps(l_discount + i);
    v_quantity = _mm256_loadu_ps(l_quantity + i);

    // Take the bitwise AND of each comparison to create a bitmask, which will select the
    // rows that pass the predicate.
    v_p0 = _mm256_castsi256_ps(_mm256_cmpgt_epi32(v_shipdate, v_shipdate_lower));
    v_p0 = _mm256_and_ps(v_p0, _mm256_castsi256_ps(_mm256_cmpgt_epi32(v_shipdate_upper, v_shipdate)));
    v_p0 = _mm256_and_ps(v_p0, _mm256_cmp_ps(v_discount, v_discount_lower, 14));
    v_p0 = _mm256_and_ps(v_p0, _mm256_cmp_ps(v_discount_upper, v_discount, 14));
    v_p0 = _mm256_and_ps(v_p0, _mm256_cmp_ps(v_quantity_upper, v_quantity, 14));

    // Load the appropriate values from extendedprice. Since this instruction zeroes out
    // the unselected lanes, we don't need to reload discount again
    v_extendedprice = _mm256_maskload_ps(l_extendedprice + i, _mm256_castps_si256(v_p0));
    v_discount = _mm256_sub_ps(_mm256_set1_ps(1), v_discount);
    v_sum = _mm256_add_ps(_mm256_mul_ps(v_extendedprice, v_discount), v_sum);
  }

  // Handle the fringe
  for (; i < length; i++) {
      if (l_shipdate[i] >= 19940101 &&
              l_shipdate[i] < 19950101 &&
              l_discount[i] >= 5 &&
              l_discount[i] <= 7 &&
              l_quantity[i] < 24) {
          result += l_extendedprice[i] * l_discount[i];
      }
  }

  float sum[8];
  _mm256_store_ps(sum, v_sum);
  for (int i = 0; i < 8; i++) {
      result += sum[i];
  }
  return result;
}

int main() {
    const char *filename = path_for_data("lineitem.tbl");
    time_t start, end;

    // Loading
    start = clock();
    struct lineitem_q6 *items = load_q6(fopen(filename, "r"), 5);
    end = clock();
    double cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("loading - (%.3f seconds)\n", cpu_time_used);

    // Processing
    start = clock();
    float result = q6(items);
    end = clock();
    double cpu_time_used_2 = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("%f (%.3f seconds)\n", result, cpu_time_used_2);
    //printf("%.3fx speedup with inlining\n", cpu_time_used / cpu_time_used_2);
}
