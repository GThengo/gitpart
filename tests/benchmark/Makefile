SRCS  := $(shell ls -rt -d -1 ls *.c)
BENCH := bench.out
LIB   := -L../.. -L../../deps/check-0.9.8/src/.libs -L/usr/lib -L/usr/local/lib \
	-lart -lcheck -lpmem -lvmem
OBJS  := $(addsuffix .o, $(basename $(SRCS)))

CFLAGS   := -I../../deps/check-0.9.8/src -I../../src -I/usr/local/include \
	-std=c99 -D_GNU_SOURCE -Wall -g -O0 -fPIC
CXXFLAGS := $(CFLAGS)
LDFLAGS  := $(LIB)

$(BENCH): $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS) 

all: $(BENCH)

clean:
	rm -f $(OBJS) $(BENCH)

