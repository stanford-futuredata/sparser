
#include <stack>

#include <immintrin.h>
#include <strings.h>

// Structural characters
#define COLON       ':'
#define RIGHT_BRACE '}'
#define RIGHT_BRACKET ']'
#define LEFT_BRACE  '{'
#define LEFT_BRACKET  '['
#define BACKSLASH   '\\'
#define QUOTE       '"'
#define COMMA       ','

// AVX256 vector size in Bytes
#define VS          32

struct character_bitmaps {
  uint64_t *colon_bm;
  uint64_t *rbrace_bm;
  uint64_t *lbrace_bm;
  uint64_t *lbracket_bm;
  uint64_t *rbracket_bm;
  uint64_t *backslash_bm;
  uint64_t *quotes_bm;
  uint64_t *comma_bm;

  // Size of the bitmap, in *bits*.
  size_t length;

  // Size of the bitmap in 8-byte words.
  size_t words;
};

// Bit manipulations from the paper.

uint64_t R(uint64_t x) {
  return x & (x - 1L);
}

uint64_t E(uint64_t x) {
  return x & (-x);
}

uint64_t S(uint64_t x) {
  return x ^ (x - 1L);
}

void mison_dbg_print_string(const char *s) {
  size_t len = strlen(s);
  for (int i = 0; i < len; i++) {
    fprintf(stderr, "%c ", s[i]);
  }
  fprintf(stderr, "\n");
}

void mison_dbg_print_mask(uint64_t mask) {
  uint64_t bits = sizeof(uint64_t) * 8;
  for (uint64_t i = 0; i < bits; i++) {
    uint32_t print = 0;
    if (mask & (0x1L << i)) {
      print = 1;
    }
    fprintf(stderr, "%u ", print);
  }
  fprintf(stderr, "\n");
}

// Step 0: Allocating bitmap space.

// Step 1: Building Structural Character Bitmaps
struct character_bitmaps *
build_character_bitmaps(const char *record, const size_t length) {
  size_t bm_count = (length / 8) / sizeof(uint32_t) + 1;
  // To enable casting to 64-bit values.
  if (bm_count % 2 == 1) {
    bm_count++;
  }
  const size_t bm_size = sizeof(uint32_t);

  // Preallocate memory for the bitmasks.
  uint32_t *colon_bm = (uint32_t *)calloc(bm_count, bm_size);
  uint32_t *rbrace_bm = (uint32_t *)calloc(bm_count, bm_size);
  uint32_t *lbrace_bm = (uint32_t *)calloc(bm_count, bm_size);
  uint32_t *backslash_bm = (uint32_t *)calloc(bm_count, bm_size);
  uint32_t *quotes_bm = (uint32_t *)calloc(bm_count, bm_size);
  uint32_t *comma_bm = (uint32_t *)calloc(bm_count, bm_size);
  uint32_t *rbracket_bm = (uint32_t *)calloc(bm_count, bm_size);
  uint32_t *lbracket_bm = (uint32_t *)calloc(bm_count, bm_size);
  
  // Comparators
  const __m256i colons = _mm256_set1_epi8(COLON);
  const __m256i rbrace = _mm256_set1_epi8(RIGHT_BRACE);
  const __m256i lbrace = _mm256_set1_epi8(LEFT_BRACE);
  const __m256i backslash = _mm256_set1_epi8(BACKSLASH);
  const __m256i quotes = _mm256_set1_epi8(QUOTE);
  const __m256i rbracket = _mm256_set1_epi8(RIGHT_BRACKET);
  const __m256i lbracket = _mm256_set1_epi8(LEFT_BRACKET);
  const __m256i comma = _mm256_set1_epi8(COMMA);

  size_t bm_index = 0;
  for (size_t i = 0; i < length; i += VS) {
    __m256i data = _mm256_loadu_si256((__m256i *)(record + i));

    uint32_t mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, colons));
    colon_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, rbrace));
    rbrace_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, lbrace));
    lbrace_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, backslash));
    backslash_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, quotes));
    quotes_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, quotes));
    quotes_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, rbracket));
    rbracket_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, lbracket));
    lbracket_bm[bm_index] = mask;

    mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, comma));
    comma_bm[bm_index] = mask;

    bm_index++;
  }
  // TODO fringing...
  
  struct character_bitmaps *b = (struct character_bitmaps *)malloc(sizeof(struct character_bitmaps));
  b->colon_bm = (uint64_t *)colon_bm;
  b->rbrace_bm = (uint64_t *)rbrace_bm;
  b->lbrace_bm = (uint64_t *)lbrace_bm;
  b->backslash_bm = (uint64_t *)backslash_bm;
  b->quotes_bm = (uint64_t *)quotes_bm;
  b->rbracket_bm = (uint64_t *)rbracket_bm;
  b->lbracket_bm = (uint64_t *)lbracket_bm;
  b->comma_bm = (uint64_t *)comma_bm;
  b->length = length;
  b->words = bm_count / 2;

#if DEBUG
  fprintf(stderr, ":\n");
  mison_dbg_print_mask(b->colon_bm[0]);

  fprintf(stderr, "}\n");
  mison_dbg_print_mask(b->rbrace_bm[0]);
  fprintf(stderr, "{\n");
  mison_dbg_print_mask(b->lbrace_bm[0]);

  fprintf(stderr, "\\\n");
  mison_dbg_print_mask(b->backslash_bm[0]);
  fprintf(stderr, "\"\n");
  mison_dbg_print_mask(b->quotes_bm[0]);

  fprintf(stderr, "]\n");
  mison_dbg_print_mask(b->rbracket_bm[0]);
  fprintf(stderr, "[\n");
  mison_dbg_print_mask(b->lbracket_bm[0]);

  fprintf(stderr, ",\n");
  mison_dbg_print_mask(b->comma_bm[0]);

#endif

  return b;
}

// Step 2.
uint64_t *
build_structural_quote_bitmap(struct character_bitmaps *b) {

  // Preallocate memory for the bitmasks.
  uint64_t *result = (uint64_t *)calloc(b->words, sizeof(uint64_t));

  const uint64_t topset = (0x1L << 63);

  // carry over after shift.
  uint64_t carry = 0x0;
  for (int i = 0; i < b->words; i++) {
    uint64_t ander = (b->quotes_bm[i] >> 1);
    ander |= carry;

    // top bit set as the carry if the least significant bit is set and gets shifted right.
    carry = ffsl(b->quotes_bm[i]) == 1 ? topset : 0x0;
    result[i] = (ander & b->backslash_bm[i]);
  }

#if DEBUG
  fprintf(stderr, "Finding '\\\"'\n");
  mison_dbg_print_mask(result[0]);
  fprintf(stderr, "***************\n");
#endif

  // TODO can this be more efficient?
  for (int i = 0; i < b->words; i++) {
    uint64_t value = result[i];
    result[i] = b->quotes_bm[i];
#if DEBUG
    mison_dbg_print_mask(result[i]);
#endif
    while (value) {
      uint64_t bit = (E(value) << 1L);
      result[i] &= ~bit;
#if DEBUG
      mison_dbg_print_mask(~bit);
#endif
      value = R(value);
    }
  }

#if DEBUG
  fprintf(stderr, "***************\n");
  fprintf(stderr, "Quote Bitmap\n");
  mison_dbg_print_mask(result[0]);
#endif
  return result;
}

// Step 3.
uint64_t *
build_string_mask_bitmap(uint64_t *quote_bitmap, size_t words) {
  const size_t bm_count = words;
  const size_t bm_size = sizeof(uint64_t);

  // Preallocate memory for the bitmasks.
  uint64_t *result = (uint64_t *)calloc(bm_count, bm_size);

  uint64_t n = 0;
  for (int i = 0; i < words; i++) {
    uint64_t m_quote = quote_bitmap[i];
    uint64_t m_string = 0x0;

    while (m_quote) {
      uint64_t m = S(m_quote);
      m_string = m_string ^ m;
      m_quote = R(m_quote);
      n++;
    }
    if (n % 2 == 1) {
      m_string = ~m_string;
    }
    result[i] = m_string;
  }

#if DEBUG
  fprintf(stderr, "String Bitmap\n");
  mison_dbg_print_mask(result[0]);
#endif
  return result;
}

// Step 4.
uint64_t **
build_leveled_colon_bitmaps(struct character_bitmaps *b, uint64_t l) {
  uint64_t **lcb = (uint64_t **)malloc(sizeof(uint64_t **) * l);

  // Copy colon bitmap to leveled colon bitmap.
  for (int i = 0; i < l; i++) {
    lcb[i] = (uint64_t *)calloc(b->words, sizeof(uint64_t));
    memcpy(lcb[i], b->colon_bm, b->words * sizeof(uint64_t));
  } 

#if DEBUG
  fprintf(stderr, "Finished memcpy of colon bitmaps.\n");
#endif

  std::stack<size_t> stack1;
  std::stack<uint64_t> stack2;

  for (uint64_t i = 0; i < b->words; i++) {
    uint64_t m_left = b->lbrace_bm[i];
    uint64_t m_right = b->rbrace_bm[i];

    // Iterate over all right braces
    while(m_right) {
      uint64_t m_rightbit = E(m_right);
      uint64_t m_leftbit = E(m_left);
      while (m_leftbit != 0 && (m_rightbit == 0 || m_leftbit < m_rightbit)) {
        stack1.push(i);
        stack2.push(m_leftbit);
        m_left = R(m_left);
        m_leftbit = E(m_left);
      }

#if DEBUG
      fprintf(stderr, "Stack size: %zu.\n", stack1.size());
      fprintf(stderr, "m_rightbit: %llu.\n", m_rightbit);
#endif

      if (m_rightbit != 0 && stack1.size() > 0) {
        uint64_t j = stack1.top();
        m_leftbit = stack2.top();
        stack1.pop();
        stack2.pop();
          if (stack1.size() > 0 && stack1.size() <= l) {
            if (i == j) {
              lcb[stack1.size() - 1][i] = lcb[stack1.size() - 1][i] & (~(m_rightbit - m_leftbit)); 
            } else {
              lcb[stack1.size() - 1][j] = lcb[stack1.size() - 1][j] & (m_leftbit - 1L);
              lcb[stack1.size() - 1][i] = lcb[stack1.size() - 1][i] & ~(m_rightbit - 1L);
              for (int k = j + 1; k < i; k++) {
                lcb[stack1.size()][k] = 0x0;
              }
            }
          }
      }
      m_right = R(m_right);

#if DEBUG
      fprintf(stderr, "m_right: %llu.\n", m_right);
#endif
    }
  }

#if DEBUG
  for (int i = 0; i < l; i++) {
    fprintf(stderr, "L%d bitmap\n", i);
    mison_dbg_print_mask(lcb[i][0]);
  }
#endif

  return lcb;
}

// Step 4.
uint64_t **
build_leveled_comma_bitmaps(struct character_bitmaps *b, uint64_t l) {
  uint64_t **lcb = (uint64_t **)malloc(sizeof(uint64_t **) * l);

  // Copy colon bitmap to leveled colon bitmap.
  for (int i = 0; i < l; i++) {
    lcb[i] = (uint64_t *)calloc(b->words, sizeof(uint64_t));
    memcpy(lcb[i], b->comma_bm, b->words * sizeof(uint64_t));
  } 

#if DEBUG
  fprintf(stderr, "Finished memcpy of comma bitmaps.\n");
#endif

  std::stack<size_t> stack1;
  std::stack<uint64_t> stack2;

  for (uint64_t i = 0; i < b->words; i++) {
    uint64_t m_left = b->lbracket_bm[i];
    uint64_t m_right = b->rbracket_bm[i];

    // Iterate over all right braces
    while(m_right) {
      uint64_t m_rightbit = E(m_right);
      uint64_t m_leftbit = E(m_left);
      while (m_leftbit != 0 && (m_rightbit == 0 || m_leftbit < m_rightbit)) {
        stack1.push(i);
        stack2.push(m_leftbit);
        m_left = R(m_left);
        m_leftbit = E(m_left);
      }

#if DEBUG
      fprintf(stderr, "Stack size: %zu.\n", stack1.size());
      fprintf(stderr, "m_rightbit: %llu.\n", m_rightbit);
#endif

      if (m_rightbit != 0 && stack1.size() > 0) {
        uint64_t j = stack1.top();
        m_leftbit = stack2.top();
        stack1.pop();
        stack2.pop();
          if (stack1.size() > 0 && stack1.size() <= l) {
            if (i == j) {
              lcb[stack1.size() - 1][i] = lcb[stack1.size() - 1][i] & (~(m_rightbit - m_leftbit)); 
            } else {
              lcb[stack1.size() - 1][j] = lcb[stack1.size() - 1][j] & (m_leftbit - 1L);
              lcb[stack1.size() - 1][i] = lcb[stack1.size() - 1][i] & ~(m_rightbit - 1L);
              for (int k = j + 1; k < i; k++) {
                lcb[stack1.size()][k] = 0x0;
              }
            }
          }
      }
      m_right = R(m_right);

#if DEBUG
      fprintf(stderr, "m_right: %llu.\n", m_right);
#endif
    }
  }

#if DEBUG
  for (int i = 0; i < l; i++) {
    fprintf(stderr, "L%d bitmap\n", i);
    mison_dbg_print_mask(lcb[i][0]);
  }
#endif

  return lcb;
}

intptr_t mison_parse(const char *record, size_t length) {

#if DEBUG
  fprintf(stderr, "Record: %s\n", record);
  fprintf(stderr, "Record Length: %ld\n", length);
#endif

  const int levels = 6;

  struct character_bitmaps *b = build_character_bitmaps(record, length);
  uint64_t *quote_bitmap = build_structural_quote_bitmap(b);

  uint64_t *str_bm = build_string_mask_bitmap(quote_bitmap, b->words);
  uint64_t **colon_res = build_leveled_colon_bitmaps(b, levels);
  uint64_t **comma_res = build_leveled_comma_bitmaps(b, levels);

  free(b->colon_bm);
  free(b->rbrace_bm);
  free(b->lbrace_bm);
  free(b->backslash_bm);
  free(b->quotes_bm);
  free(b->rbracket_bm);
  free(b->lbracket_bm);
  free(b->comma_bm);
  free(b);
  free(quote_bitmap);

  free(str_bm);

  for (int i = 0; i < levels; i++) {
    free(colon_res[i]);
    free(comma_res[i]);
  }

  free(colon_res);
  free(comma_res);

  return (intptr_t)colon_res + (intptr_t)(comma_res) + (intptr_t)str_bm;
}
