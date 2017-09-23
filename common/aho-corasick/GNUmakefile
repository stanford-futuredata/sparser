~ := acism
include $(word 1, ${RULES} rules.mk)

#---------------- PRIVATE VARS:
acism.x         = acism_x acism_mmap_x

#---------------- PUBLIC (see rules.mk):
all             : libacism.a
test            : acism_t.pass
install         : libacism.a  acism.h
clean           += *.tmp

#---------------- PRIVATE RULES:
libacism.a      : acism.o  acism_create.o  acism_dump.o  acism_file.o
acism_t.pass    : ${acism.x}  words
${acism.x}      : libacism.a  msutil.o  tap.o

#_CFLAGS = -DACISM_SIZE=8

# vim: set nowrap :
