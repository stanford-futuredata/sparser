/*
** Copyright (C) 2009-2014 Mischa Sandberg <mischasan@gmail.com>
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU Lesser General Public License as
** published by the Free Software Foundation.  You may not use, modify or
** distribute this program under any other version of the GNU Lesser General
** Public License.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU Lesser General Public License for more details.
**
** You should have received a copy of the GNU Lesser General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/

#ifndef ACISM_H
#define ACISM_H

// "acism" uses MEMREF {ptr,len} bytevec structs for "string" args,
// rather than NUL-terminated "C" strings.

#ifndef MSUTIL_H
#include <stdio.h>
typedef struct { char const *ptr; size_t len; } MEMREF;
#endif

typedef struct acism ACISM;

ACISM* acism_create(MEMREF const *strv, int nstrs);
void   acism_destroy(ACISM*);

// For each match, acism_scan calls its ACISM_ACTION fn,
//  giving it the strv[] index of the matched string,
//  and the text[] offset of the byte PAST the end of the string.
// If ACISM_ACTION returns 0, search continues; otherwise,
//  acism_more returns that nonzero value immediately.

typedef int (ACISM_ACTION)(int strnum, int textpos, void *context);

// If sequential blocks of (text) are passed to repeated acism_more calls,
//  then search continues where the previous acism_more left off --
//  string matches can cross block boundaries.
// *state should initially be (0).

int acism_more(ACISM const*, MEMREF const text,
                 ACISM_ACTION *fn, void *fndata, int *state);

static inline int acism_scan(ACISM const*psp, MEMREF const text,
                               ACISM_ACTION *fn, void *fndata)
{
    int state = 0;
    return acism_more(psp, text, fn, fndata, &state);
}

void   acism_save(FILE*, ACISM const*);
ACISM* acism_load(FILE*);
ACISM* acism_mmap(FILE*);

// diagnostics
typedef enum {
    PS_STATS=1, PS_TRAN=2, PS_HASH=4, PS_TREE=8, PS_ALL=-1
} PS_DUMP_TYPE;

// If (pattv) is not NULL, dump output includes strings.
void acism_dump(ACISM const*, PS_DUMP_TYPE, FILE*, MEMREF const*pattv);

#define ACISM_STATS 1   // Collect perf stats during acism_create (see acism_dump).

#endif//ACISM_H
