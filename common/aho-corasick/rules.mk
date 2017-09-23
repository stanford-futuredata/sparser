# Environment:
#-IMPORT VARS
#   FORMAT                 One of {,debug,cover,profile}. Needs expansion
#   CFLAGS_,_CFLAGS     Prefix and suffix of ${CFLAGS}
#   CPPFLAGS_,CXXFLAGS_,LDFLAGS_,LDLIBS_,...
#   clean               Additional files for "make clean".
#   env                 Variables of interest to the gmake user...
#
#-EXPORT VARS
#   COMMA           "," -- useful in $(...) commands
#   SPACE           " "
#   OS              OS name (AIX, Darwin, HP-UX, Linux, Solaris)
#   P4_ROOT         P4 client root directory
#   PREFIX          Global imports directory (default: /usr/local)
#   DESTDIR         Global exports directory (default: ${PREFIX})
#   CONFIG          Combination of all settings that affect compiled binary formats.
#
#-TARGETS
#   all clean tags test  ... The usual.
#
#   install         Install dependent .{a,h,so} files in ${DESTDIR}/{lib,include,lib}
#   install.bin     Install dependencies in ${DESTDIR}/bin
#
#-ACTIONS
#   defs            Print all builtin compiler macros (e.g. __LONG_MAX__, __x86_64)
#   env             Print <var>=<value> for every name in ${env}
#   sh              Execute ${SHELL} in the makefile's environment
#   source          List all source files
#
# NOTES
#   LDFLAGS,LDLIBS should *only* be used for standard OS libraries.
#   Any other dependencies are flaky, with the usual security issues of -rpath/LD_LIBRARY_PATH
#-----------------------------------------------------------------------------

# Each module sets "~ := <name>"  (note ":=" not "=").
#   A parent makefile will set <name> to the module's directory.
#   This sets $. = ${<name>}, defaulting to "."

. := $(word 1,${$~} .)
-include $./*.d
# clean uses ${all} to delete *.d
all             += $~

ifndef RULES_MK
RULES_MK        := 1
RULES           := $(word 1,${RULES} $./rules.mk)

export LD_LIBRARY_PATH

COMMA           = ,
OS              = $(shell uname -s)
PS4             = \# # Prefix for "sh -x" output.
SHELL           = bash
SPACE           = $` # " "
PREFIX          ?= /usr/local
DESTDIR         ?= ${PREFIX}

CONFIG          ?= $(call Join,_,${OS} ${ARCH} $(filter-out cc gcc,$(notdir ${CC})) ${FORMAT})

#-----------------------------------------------------------------------------
ARCH.Darwin    ?= universal
ARCH           ?= ${ARCH.${OS}}

BITS.Darwin.universal   = Universal
BITS.Darwin.x8664       = x8664
BITS.Linux.x8664        = 64
BITS                    = ${BITS.${OS}.${ARCH}}

#--- *.${FORMAT}:
# For gcc 4.5+, use: -O3 -flto --- link-time optimization including inlining. Incompatible with -g.

CFLAGS.         = -O3
CFLAGS.cover    = --coverage -DNDEBUG
LDFLAGS.cover   = --coverage
CFLAGS.debug    = -O0 -Wno-uninitialized
CPPFLAGS.debug  = -UNDEBUG
CPPLAGS.profile = -DNDEBUG
CFLAGS.profile  = -pg
LDFLAGS.profile = -pg

# PROFILE tests get stats on syscalls appended to their .pass files.
#   Darwin spells strace "dtruss".
exec.profile    = $(shell which dtruss strace) -cf

#--- *.${OS}:
#XXX HP-UX gcc 4.2.3 does not grok -fstack-protector. Sigh.
CFLAGS.AIX      = -fstack-protector --param ssp-buffer-size=4
CFLAGS.Darwin   = ${CFLAGS.AIX}
CFLAGS.HP-UX    =
CFLAGS.Linux    = ${CFLAGS.AIX}
CFLAGS.SunOS    = ${CFLAGS.AIX}

#LDLIBS.Darwin   += -lstdc++.6
#LDLIBS.Linux   += /usr/lib/libstdc++.so.6

LDLIBS.Linux    = -lm
LDLIBS.FreeBSD  = -lm

#-----------------------------------------------------------------------------
# -fPIC allows all .o's to be built into .so's.

CFLAGS          += ${CFLAGS_} -g -MMD -fPIC -pthread -fdiagnostics-show-option -fno-strict-aliasing
CFLAGS          += -Wall -Wextra -Wcast-align -Wcast-qual -Wformat=2 -Wformat-security -Wmissing-prototypes -Wnested-externs -Wpointer-arith -Wshadow -Wstrict-prototypes -Wunused -Wwrite-strings
CFLAGS          += -Wno-attributes -Wno-cast-qual -Wno-error -Wno-unknown-pragmas -Wno-unused-parameter
CFLAGS          += ${CFLAGS.${FORMAT}} ${CFLAGS.${OS}} ${CFLAGS.${FORMAT}.${OS}} ${_CFLAGS}

CPPFLAGS        += ${CPPFLAGS_} -I${PREFIX}/include -D_FORTIFY_SOURCE=2 -D_GNU_SOURCE ${CPPFLAGS.${FORMAT}} ${CPPFLAGS.${OS}} ${_CPPFLAGS}
CXXFLAGS        += ${CXXFLAGS_} $(filter-out -Wmissing-prototypes -Wnested-externs -Wstrict-prototypes, ${CFLAGS}) ${_CXXFLAGS}
LDFLAGS         += ${LDFLAGS_}  -pthread  -L${PREFIX}/lib  ${LDFLAGS.${FORMAT}}  ${LDFLAGS.${OS}}  ${_LDFLAGS}
LDLIBS          += ${LDLIBS_}   -lstdc++  ${LDLIBS.${OS}}   ${_LDLIBS}
 
#-----------------------------------------------------------------------------
# Explicitly CANCEL THESE EVIL BUILTIN RULES:
%               : %.c
%               : %.cpp
%.c             : %.l
%.c             : %.y
%.r             : %.l

#-----------------------------------------------------------------------------
.PHONY          : all clean cover debug defs env install profile sh source tags test
.DEFAULT_GOAL   := all

# ${all} contains subproject names. It can be used in ACTIONS but not RULES,
#   since it accumulates across every "include <submakefile>"
# ${junkfiles} is how to get metachars (commas) through the ${addsuffix...} call.
junkfiles       = {gmon.out,tags,*.[dis],*.fail,*.gcda,*.gcno,*.gcov,*.prof}

all             :;@echo "$@ done for FORMAT='${FORMAT}'"

clean           :;@( ${MAKE} -nps all install test | sed -n '/^# I/,$${/^[^\#\[%.][^ %]*: /s/:.*//p;}'; \
                    echo ${clean} $(addsuffix /${junkfiles}, $(foreach _,${all},${$_})) $(filter %.d,${MAKEFILE_LIST}) ) | xargs rm -rf

cover           : FORMAT := cover
%.cover         : %.test    ; gcov -bcp ${$@} | covsum

# Expand: translate every occurrence of "${var}" in a file to its env value (or ""):
#   E.G.   ${Expand} foo.tmpl >foo.ini
Expand          = perl -pe 's/ (?<!\\) \$${ ([A-Z_][A-Z_0-9]*) } / $$ENV{$$1} || ""/geix'
Join            = $(subst ${SPACE},$1,$(strip $2))

# Install: do the obvious for include and lib; "bin" files are not obvious ...
Install         = $(if $2, mkdir -p $1; pax -rwpe -s:.*/:: $2 $1)
install         : $(addprefix install., bin man1 man3 sbin) \
                ; $(call Install,${DESTDIR}/include, $(filter %.h,$^) $(filter %.hpp,$^)) \
                ; $(call Install,${DESTDIR}/lib,     $(filter %.a,$^) $(filter %.so,$^) $(filter %.dylib,$^))

# install.% cannot be .PHONY because there is no pattern expansion of phony targets.
install.bin     :           ; $(call Install,${DESTDIR}/bin,$^)
install.man%    :           ; $(call Install,${DESTDIR}/man/$(subst install.,,$@),$^)
install.%       :           ; $(call Install,${DESTDIR}/$(subst install.,,$@),$^)

profile         : FORMAT := profile
profile         : test      ;@for x in ${$*.test:.pass=}; do case `file $$x` in *script*) ;; *) gprof -b $$x >$$x.prof; esac; done

%.test          : ${%.test}

# GMAKE trims leading "./" from $* ; ${*D}/${*F} restores it, so no need to fiddle with $PATH.
%.pass          : %         ; rm -f $@; ${exec.${FORMAT}} ${*D}/${*F} >& $*.fail && mv -f $*.fail $@

%.so            : %.o       ; ${CC} ${LDFLAGS} -o $@ -shared $< ${LDLIBS}
%.so            : %.a       ; ${CC} ${CFLAGS}  -o $@ -shared -Wl,-whole-archive $< ${LDLIBS} -Wl,-no-whole-archive
%.a             :           ; $(if $(filter %.o,$^), ar crs $@ $(filter %.o,$^))
%.yy.c          : %.l       ; flex -o $@ $<
%.tab.c         : %.y       ; bison $<
%/..            :           ;@mkdir -p ${@D}
%               : %.gz      ; gunzip -c $^ >$@

# Ensure that intermediate files (e.g. the foo.o caused by "foo : foo.c")
#  are not auto-deleted --- causing a re-compile every second "make".
.SECONDARY      :

#---------------- Handy make-related commands.
# defs - list gcc's builtin macros
# env - environment relevant to the make. "sort -u" because env entries may not be unique -- "env +=" in multiple makefiles.
# sh - invoke a shell within the makefile's env:
# source - list files used and not built by the "make". Explicitly filters out "*.d" files.
# NOTE: "make tags" BEFORE "make all" is incomplete because *.h dependencies are only in *.d files.

defs            :;@${CC} ${CPPFLAGS} -E -dM - </dev/null | cut -c8- | sort
env             :;@($(foreach _,${env},echo $_=${$_};):) | sort -u
sh              :;@PS1='${PS1} [make] ' ${SHELL}
source          :;@$(if $^, ls $^;) ${MAKE} -nps all test cover profile | sed -n '/^. Not a target/{ n; /^install/d; /^[^ ]*\.d:/!{ /^[^.*][^ ]*:/s/:.*//p; }; }' | sort -u
tags            :; ctags -B $(filter %.c %.cpp %.h %.hpp, $^)

# "make SomeVar." prints ${SomeVar}
%.              :;@echo '${$*}'

#TODO: somehow pick up all the target-specific flags for *.o, for *.[Isi]

# %.I lists all (recursive) #included files; e.g.: "make /usr/include/errno.h.I"
%.I             : %.c       ;@ls -1 2>&- `${CC}  ${CPPFLAGS} ${TARGET_ARCH} -M $<` | sort -u
%.I             : %.cpp     ;@ls -1 2>&- `${CXX} ${CPPFLAGS} ${TARGET_ARCH} -M $<` | sort -u
%.i             : %.c       ; ${COMPILE.c}   -E -o $@ $<
%.i             : %.cpp     ; ${COMPILE.cpp} -E -o $@ $<
%.s             : %.c       ; $(filter-out -Werror,${COMPILE.c}) -DNDEBUG -S -o $@ $< && deas $@
%.s             : %.cpp     ; ${COMPILE.cpp} -S -o $@ $< && deas $@

endif # RULES_MK
# vim: set nowrap :
