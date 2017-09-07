/**
 * bitmap.h
 *
 * A simple bitmap implementation.
 *
 */

#ifndef _BITMAP_H_
#define _BITMAP_H_

// for popcnt
#include <immintrin.h>

#define WORDSZ 64

typedef struct {
  uint64_t *bits;

  size_t capacity;
  size_t words;
  size_t count;
} bitmap_t;


/** Initialize a new bitmap with `bits` capacity. */
bitmap_t bitmap_new(size_t bits) {
  size_t words = (bits / WORDSZ) + 1;

  bitmap_t m;
  m.bits = (uint64_t *)calloc(words, sizeof(uint64_t));
  m.words = words;
  m.capacity = bits;
  m.count = 0;
  return m;
}

/** Free a bitmap. */
void bitmap_free(bitmap_t *bm) {
  free(bm->bits);
}

/** Clear all bits in the bitmap. */
void bitmap_reset(bitmap_t *bm) {
  memset(bm->bits, 0, sizeof(uint64_t) * bm->words);
  bm->count = 0;
}

/** Set bit `index` in the bitmap. */
void bitmap_set(bitmap_t *bm, unsigned long index) {
  size_t word = index / WORDSZ;
  unsigned shift = index % WORDSZ;
  bm->bits[word] |= (0x1L << shift);
}

/** Unset bit `index` in the bitmap. */
void bitmap_unset(bitmap_t *bm, unsigned long index) {
  size_t word = index / WORDSZ;
  unsigned shift = word % WORDSZ;
  bm->bits[word] &= ~(0x1L << shift);
}

/** Return a new bitmap which the bitwise & of `bm1` and `bm2`. */
bitmap_t bitmap_and(bitmap_t *bm1, const bitmap_t *bm2) {
  assert(bm1->capacity == bm2->capacity);

  bitmap_t m = bitmap_new(bm1->capacity); 

  for (int i = 0; i < bm1->words; i++) {
    m.bits[i] = bm1->bits[i] & bm2->bits[i];
    m.count += _mm_popcnt_u64(m.bits[i]);
  }

  return m;
}

unsigned long bitmap_count(bitmap_t *bm) {
  return bm->count;
}

#endif

