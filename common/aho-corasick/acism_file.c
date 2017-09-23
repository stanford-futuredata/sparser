/*
** Copyright (C) 2009-2014 Mischa Sandberg <mischasan@gmail.com>
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

#include "msutil.h"
#include "_acism.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>

#ifndef MAP_NOCORE
# define MAP_NOCORE 0
#endif

void
acism_save(FILE *fp, ACISM const*psp)
{
    ACISM ps = *psp;
    // Overwrite pointers with magic signature.
    assert(8 <= sizeof ps.tranv + sizeof ps.hashv);
    memcpy(&ps, "ACMischa", 8);
    ps.flags &= ~IS_MMAP;
    fwrite(&ps, sizeof(ACISM), 1, fp);
    fwrite(psp->tranv, p_size(psp), 1, fp);
}

ACISM*
acism_load(FILE *fp)
{
    ACISM *psp = (ACISM *)calloc(sizeof(ACISM), 1);

    if (fread(psp, sizeof(ACISM), 1, fp) == 1
            && !memcmp(psp, "ACMischa", 8)
            && (set_tranv(psp, malloc(p_size(psp))), 1)
            && fread(psp->tranv, p_size(psp), 1, fp)) {
        return psp;
    }

    acism_destroy(psp);
    return NULL;
}

ACISM*
acism_mmap(FILE *fp)
{
    ACISM *mp = (ACISM *)mmap(0, lseek(fileno(fp), 0L, 2), PROT_READ,
                    MAP_SHARED|MAP_NOCORE, fileno(fp), 0);
    if (mp == MAP_FAILED) return NULL;

    ACISM *psp = (ACISM *)malloc(sizeof*psp);
    *psp = *(ACISM*)mp;
    psp->flags |= IS_MMAP;
    if (memcmp(psp, "ACMischa", 8)) {
        acism_destroy(psp);
        return NULL;
    }

    set_tranv(psp, ((char *)mp) + sizeof(ACISM));
    return psp;
}

void
acism_destroy(ACISM *psp)
{
    if (!psp) return;
    if (psp->flags & IS_MMAP)
        munmap((char*)psp->tranv - sizeof(ACISM),
               sizeof(ACISM) + p_size(psp));
    else free(psp->tranv);
    free(psp);
}
//EOF
