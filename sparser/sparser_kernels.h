/** sparser_kernels.h
 *
 * Example implementations of the substring-search RF.
 *
 * XXX Currently not used in favor of glibc strstr (which does something
 * similar for small strings -- the glibc strstr/memmem implementation considers
 * strings < 32 bytes small and uses a SIMD-based implementation of memchr +
 * the Two-Way string search algorithm, resulting in similar performance for
 * ASCII-based foramts. For binary formats, the RFs below may offer an additional
 * speed boost).
 *
 * The plan to switch back to using these later, when we add back support for binary
 * formats.
 *
 */

#ifndef _SPARSER_KERNELS_H_
#define _SPARSER_KERNELS_H_

#include <immintrin.h>

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
inline int search_epi32(__m256i reg, const char *base) {
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

#endif
