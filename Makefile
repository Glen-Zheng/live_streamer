#########################################################################
# ECE252 Lab Makefile for paster
# Adapted from Y. Huang, improved 2025/11/12
#########################################################################

CC      = gcc
CFLAGS  = -Wall -g -Iinclude `xml2-config --cflags`
LD      = gcc
LDFLAGS = -g
LIBS    = -lz -lcurl -lpthread `xml2-config --libs`

# Source files
SUBDIR     = src
SRCS       = $(SUBDIR)/findpng2.c
LIB_SRCS   = include/lab_png.c

# Object files
OBJS       = $(SRCS:.c=.o)
LIB_OBJS   = $(LIB_SRCS:.c=.o)
ALL_OBJS   = $(OBJS) $(LIB_OBJS)

# Dependency files
DEPS       = $(SRCS:.c=.d)
LIB_DEPS   = $(LIB_SRCS:.c=.d)
ALL_DEPS   = $(DEPS) $(LIB_DEPS)

# Executables
EXES       = findpng2

# Default target
all: $(EXES)

# Link rule
findpng2: $(OBJS) $(LIB_OBJS)
	$(LD) $(LDFLAGS) -o $@ $^ $(LIBS)

# Compile rule (.c → .o)
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Dependency rule (.c → .d)
%.d: %.c
	@$(CC) -MM -MF $@ -MT '$(@:.d=.o)' $(CFLAGS) $<

# Include dependency files if they exist
-include $(ALL_DEPS)

.PHONY: clean
clean:
	rm -f $(OBJS) $(LIB_OBJS) $(ALL_DEPS) $(EXES)
