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

/** Copies a bitmap. */
bitmap_t bitmap_from(bitmap_t *bm) {
	bitmap_t m = bitmap_new(bm->capacity);
	memcpy(m.bits, bm->bits, m.words * sizeof(uint64_t));
	m.count = bm->count;
	return m;
}

/** Copies a bitmap. */
void bitmap_copy(bitmap_t *dst, bitmap_t *src) {
	memcpy(dst->bits, src->bits, src->words * sizeof(uint64_t));
	dst->words = src->words;
	dst->capacity = src->capacity;
	dst->count = src->count;
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
	bm->count++;
}

/** Unset bit `index` in the bitmap. */
void bitmap_unset(bitmap_t *bm, unsigned long index) {
  size_t word = index / WORDSZ;
  unsigned shift = word % WORDSZ;
  bm->bits[word] &= ~(0x1L << shift);
	bm->count--;
}

/** Return a new bitmap which the bitwise & of `bm1` and `bm2`. */
void bitmap_and(bitmap_t *result, bitmap_t *bm1, const bitmap_t *bm2) {
  assert(bm1->capacity == bm2->capacity);
  assert(result->capacity == bm1->capacity);

  result->count = 0;
  for (unsigned i = 0; i < bm1->words; i++) {
    result->bits[i] = bm1->bits[i] & bm2->bits[i];
    result->count += _mm_popcnt_u64(result->bits[i]);
  }
}

// assumes little endian
// https://stackoverflow.com/questions/111928/is-there-a-printf-converter-to-print-in-binary-format
void printBits(size_t const size, void const * const ptr) {
	unsigned char *b = (unsigned char*) ptr;
	unsigned char byte;
	int i, j;

	for (i=size-1;i>=0;i--)
	{
		for (j=7;j>=0;j--)
		{
			byte = (b[i] >> j) & 1;
			fprintf(stderr, "%u", byte);
		}
	}
}


void bitmap_print(bitmap_t *bm) {
	for (unsigned i = 0; i < bm->words; i++) {
		printBits(sizeof(bm->bits[i]), &bm->bits[i]);
	}
	fprintf(stderr, "\n");
}

unsigned long bitmap_capacity(bitmap_t *bm) {
  return bm->capacity;
}

unsigned long bitmap_count(bitmap_t *bm) {
  return bm->count;
}

#endif

