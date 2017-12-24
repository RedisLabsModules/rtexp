# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS ?= -g -pg -no-pie -fPIC -lc -lm -O3 -std=gnu99 -I$(RM_INCLUDE_DIR) -Wall -Wno-unused-function \
		-DREDISMODULE_EXPERIMENTAL_API 

CC=gcc

OBJS=util.o strings.o sds.o vector.o alloc.o periodic.o

all: librmutil.a

clean:
	rm -rf *.o *.a

librmutil.a: $(OBJS)
	ar rcs $@ $^

test_vector: test_vector.o vector.o
	$(CC) -Wall -o $@ $^ -lc -lpthread -O0
	@(sh -c ./$@)
.PHONY: test_vector

test_periodic: test_periodic.o periodic.o
	$(CC) -Wall -o $@ $^ -lc -lpthread -O0
	@(sh -c ./$@)
.PHONY: test_periodic
	
test: test_periodic test_vector
.PHONY: test
