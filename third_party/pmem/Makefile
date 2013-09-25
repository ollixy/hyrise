INCLUDE = -I. -Iicount -Ilibpmem -Ilibpmemalloc -Iutil
CXXFLAGS = -O3 -fPIC
PMEMOBJS = pmem.o pmem_cl.o pmem_fit.o pmem_msync.o util.o
PMEMALLOCOBJS = pmemalloc.o util.o icount.o

all: libpmem.so.1 libpmemalloc.so.1

libpmem.so.1: $(PMEMOBJS)
	$(CC) -shared -Wl,-soname,libpmem.so.1 -o $@ $(PMEMOBJS)

libpmemalloc.so.1: $(PMEMALLOCOBJS)
	$(CC) -shared -Wl,-soname,libpmemalloc.so.1 -o $@ $(PMEMALLOCOBJS)

pmem.o: libpmem/pmem.c
	$(CC) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

pmem_cl.o: libpmem/pmem_cl.c
	$(CC) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

pmem_fit.o: libpmem/pmem_fit.c
	$(CC) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

pmem_msync.o: libpmem/pmem_msync.c
	$(CC) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

pmemalloc.o: libpmemalloc/pmemalloc.c
	$(CC) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

icount.o: icount/icount.c
	$(CC) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

util.o: util/util.c
	$(CC) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

clean:
	rm -f *.o *.so.1

install: all
	cp -f *.so.1 /usr/local/lib
	ln -f -s /usr/local/lib/libpmem.so.1 /usr/local/lib/libpmem.so
	ln -f -s /usr/local/lib/libpmemalloc.so.1 /usr/local/lib/libpmemalloc.so
	ldconfig
	cp -f pmem*.h /usr/local/include

.PHONY: clean install
