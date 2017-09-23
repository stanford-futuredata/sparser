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
// msutil: generic utility functions not dependent on any engine components.

#include "msutil.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>    // open O_RDONLY
#include <stdarg.h>
#include <sys/stat.h>

char const *errname[] = {
    /*_00*/ "",              "EPERM",           "ENOENT",       "ESRCH",           "EINTR",
    /*_05*/ "EIO",           "ENXIO",           "E2BIG",        "ENOEXEC",         "EBADF",
    /*_10*/ "ECHILD",        "EAGAIN",          "ENOMEM",       "EACCES",          "EFAULT",
    /*_15*/ "ENOTBLK",       "EBUSY",           "EEXIST",       "EXDEV",           "ENODEV",
    /*_20*/ "ENOTDIR",       "EISDIR",          "EINVAL",       "ENFILE",          "EMFILE",
    /*_25*/ "ENOTTY",        "ETXTBSY",         "EFBIG",        "ENOSPC",          "ESPIPE",
    /*_30*/ "EROFS",         "EMLINK",          "EPIPE",        "EDOM",            "ERANGE",
#if   defined(__FreeBSD__)
    /*_35*/ "EDEADLK",       "EINPROGRESS",     "EALREADY",     "ENOTSOCK",        "EDESTADDRREQ",
    /*_40*/ "EMSGSIZE",      "EPROTOTYPE",      "ENOPROTOOPT",  "EPROTONOSUPPORT", "ESOCKTNOSUPPORT",
    /*_45*/ "EOPNOTSUPP",    "EPFNOSUPPORT",    "EAFNOSUPPORT", "EADDRINUSE",      "EADDRNOTAVAIL",
    /*_50*/ "ENETDOWN",      "ENETUNREACH",     "ENETRESET",    "ECONNABORTED",    "ECONNRESET",
    /*_55*/ "ENOBUFS",       "EISCONN",         "ENOTCONN",     "ESHUTDOWN",       "ETOOMANYREFS",
    /*_60*/ "ETIMEDOUT",     "ECONNREFUSED",    "ELOOP",        "ENAMETOOLONG",    "EHOSTDOWN",
    /*_65*/ "EHOSTUNREACH",  "ENOTEMPTY",       "EPROCLIM",     "EUSERS",          "EDQUOT",
    /*_70*/ "ESTALE",        "EREMOTE",         "EBADRPC",      "ERPCMISMATCH",    "EPROGUNAVAIL",
    /*_75*/ "EPROGMISMATCH", "EPROCUNAVAIL",    "ENOLCK",       "ENOSYS",          "EFTYPE",
    /*_80*/ "EAUTH",         "ENEEDAUTH",       "EIDRM",        "ENOMSG",          "EOVERFLOW",
    /*_85*/ "ECANCELED",     "EILSEQ",          "ENOATTR",      "EDOOFUS",         "EBADMSG",
    /*_90*/ "EMULTIHOP",     "ENOLINK",         "EPROTO"                           
#elif defined(__linux__)
    /*_35*/ "EDEADLK",       "ENAMETOOLONG",    "ENOLCK",       "ENOSYS",          "ENOTEMPTY",
    /*_40*/ "ELOOP",         "E041",            "ENOMSG",       "EIDRM",           "ECHRNG",
    /*_45*/ "EL2NSYNC",      "EL3HLT",          "EL3RST",       "ELNRNG",          "EUNATCH",
    /*_50*/ "ENOCSI",        "EL2HLT",          "EBADE",        "EBADR",           "EXFULL",
    /*_55*/ "ENOANO",        "EBADRQC",         "EBADSLT",      "E058",            "EBFONT",
    /*_60*/ "ENOSTR",        "ENODATA",         "ETIME",        "ENOSR",           "ENONET",
    /*_65*/ "ENOPKG",        "EREMOTE",         "ENOLINK",      "EADV",            "ESRMNT",
    /*_70*/ "ECOMM",         "EPROTO",          "EMULTIHOP",    "EDOTDOT",         "EBADMSG",
    /*_75*/ "EOVERFLOW",     "ENOTUNIQ",        "EBADFD",       "EREMCHG",         "ELIBACC",
    /*_80*/ "ELIBBAD",       "ELIBSCN",         "ELIBMAX",      "ELIBEXEC",        "EILSEQ",
    /*_85*/ "ERESTART",      "ESTRPIPE",        "EUSERS",       "ENOTSOCK",        "EDESTADDRREQ",
    /*_90*/ "EMSGSIZE",      "EPROTOTYPE",      "ENOPROTOOPT",  "EPROTONOSUPPORT", "ESOCKTNOSUPPORT",
    /*_95*/ "EOPNOTSUPP",    "EPFNOSUPPORT",    "EAFNOSUPPORT", "EADDRINUSE",      "EADDRNOTAVAIL",
    /*100*/ "ENETDOWN",      "ENETUNREACH",     "ENETRESET",    "ECONNABORTED",    "ECONNRESET",
    /*105*/ "ENOBUFS",       "EISCONN",         "ENOTCONN",     "ESHUTDOWN",       "ETOOMANYREFS",
    /*110*/ "ETIMEDOUT",     "ECONNREFUSED",    "EHOSTDOWN",    "EHOSTUNREACH",    "EALREADY",
    /*115*/ "EINPROGRESS",   "ESTALE",          "EUCLEAN",      "ENOTNAM",         "ENAVAIL",
    /*120*/ "EISNAM",        "EREMOTEIO",       "EDQUOT",       "ENOMEDIUM",       "EMEDIUMTYPE",
    /*125*/ "ECANCELED",     "ENOKEY",          "EKEYEXPIRED",  "EKEYREVOKED",     "EKEYREJECTED",
    /*130*/ "EOWNERDEAD",    "ENOTRECOVERABLE", "ERFKILL"                          
#endif
};
int const nerrnames = sizeof(errname)/sizeof(*errname);

MEMBUF
membuf(int size)
{ return size ? (MEMBUF){calloc(size+1, 1), size} : NILBUF; }

void
buffree(MEMBUF buf)
{ free(buf.ptr); }

int
nilbuf(MEMBUF buf)
{ return !buf.ptr; }

int
nilref(MEMREF const ref)
{ return !ref.len && !ref.ptr; }

MEMREF
memref(char const *mem, int len)
{ return mem && len ? (MEMREF){mem,len} : NILREF; }

MEMREF
bufref(MEMBUF const buf)
{ return (MEMREF) {buf.ptr, buf.len}; }

MEMBUF
chomp(MEMBUF buf)
{
    if (buf.ptr)
	while (buf.len > 0 && isspace(buf.ptr[buf.len - 1]))
	    buf.ptr[--buf.len] = 0;
    return  buf;
}

void
die(char const *fmt, ...)
{
    va_list	vargs;
    va_start(vargs, fmt);
    if (*fmt == ':') fputs(getprogname(), stderr);
    vfprintf(stderr, fmt, vargs);
    va_end(vargs);
    if (fmt[strlen(fmt)-1] == ':')
        fprintf(stderr, " %s %s", errname[errno], strerror(errno));
    putc('\n', stderr);
    _exit(1);
}

#if defined(__linux__)
char const *
getprogname(void)
{
    static char *progname;

    if (!progname) {
        char    buf[999];
        int     len;
        sprintf(buf, "/proc/%d/exe", getpid());
        len = readlink(buf, buf, sizeof(buf));
        if (len < 0 || len == sizeof(buf))
            return NULL;
        buf[len] = 0;
        char    *cp = strrchr(buf, '/');
        progname = strdup(cp ? cp + 1 : buf);
    }

    return  progname;
}
#endif

MEMREF *
refsplit(char *text, char sep, int *pcount)
{
    char	*cp;
    int		i, nstrs = 0;
    MEMREF      *strv = NULL;

    if (*text) {
        for (cp = text, nstrs = 1; (cp = strchr(cp, sep)); ++cp) 
            ++nstrs;

        strv = malloc(nstrs * sizeof(MEMREF));

        for (i = 0, cp = text; (cp = strchr(strv[i].ptr = cp, sep)); ++i, ++cp) {
            strv[i].len = cp - strv[i].ptr;
            *cp = 0;
        }

        strv[i].len = strlen(strv[i].ptr);
    }

    if (pcount) 
	*pcount = nstrs;
    return    strv;
}

MEMBUF
slurp(const char *filename)
{
    MEMBUF      ret = NILBUF;
    int         fd = filename && *filename && strcmp(filename, "-") ? open(filename, O_RDONLY) : 0;
    struct stat s;
    if (fd < 0 || fstat(fd, &s))
        goto ERROR;

    if (S_ISREG(s.st_mode)) {
        ret = membuf(s.st_size);
	if (ret.len != (unsigned)read(fd, ret.ptr, ret.len))
            goto ERROR;

    } else {
        int     len, size = 4096;
        ret.ptr = malloc(size + 1);
        for (;0 < (len = read(fd, ret.ptr+ret.len, size-ret.len)); ret.len += len)
            if (len == size - (int)ret.len)
                ret.ptr = realloc(ret.ptr, (size <<= 1) + 1);
        if (len < 0)
            goto ERROR;
        ret.ptr = realloc(ret.ptr, ret.len + 1);
    }

    close(fd);
    ret.ptr[ret.len] = 0;
    return  ret;

ERROR:
    if (fd >= 0) close(fd);
    buffree(ret);
    return NILBUF;
}

double tick(void)
{
    struct timeval t;
    gettimeofday(&t, 0);
    return t.tv_sec + 1E-6 * t.tv_usec;
}

void
usage(char const *str)
{
    fprintf(stderr, "Usage: %s %s\n", getprogname(), str);
    _exit(2);
}
