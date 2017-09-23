/*
** Copyright (C) 2009-2013 Mischa Sandberg <mischasan@gmail.com>
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU Lesser General Public License Version 3 as
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

// acism_strings: reconstruct the input to acism_create.
// Returns: number of strings in strv[].
// strv[] entries are returned in lexical order, not
// necessarily the original order given to acism_create.
// strv[0].ptr points to a contiguous data block to which
// all strv[].ptrs refer.
// The data block size (in bytes) is:
// strv[nstrs-1].ptr - strv[0].ptr + strv[nstrs-1].len
// or alternately, the sum of strv[].len values.

#include "_acism.h"

typedef struct { int *seqv; MEMREF *strp; char *data; } VISIT;

static void do(VISIT*, ACISM const *psp, TRAN base, int depth);

MEMREF*
acism_strings(ACISM const *psp, int *pnstrs)
{
    int seqv[psp->nsyms];
    char *data = malloc(psp->nchars);
    MEMREF *strv = malloc(psp->nstrs * sizeof*strv);
    VISIT vt = { seqv, strv, data };

    for (i = j = 0; i < 256; i++) if (psp->symv[i]) vt.seqv[j++] = i;
    vt.seqv[j] = 0;

    do(&vt, psp, 0, 0);

    *pnstrs = psp->nstrs;
    return strv;
}

static void
do(VISIT *vp, ACISM const *psp, TRAN base, int depth)
{
    for (i = 0; i < psp->nsyms; ++i) {
        TRAN t = t_trans(psp, base, seqv[i]);
        if (!t_valid(psp, t))
            continue;
        //XXX do something!
    }
}
