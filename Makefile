CC=g++44
OPTS=-O0 -fexceptions -std=c++0x  -march=x86-64 -g
LD=-lmpio -lmsgpack -lmsgpackc -pthread -lboost_program_options
WARNS= -W -Wall -Wextra -Wformat=2 -Wstrict-aliasing=4 -Wcast-qual -Wcast-align \
	-Wwrite-strings -Wfloat-equal -Wpointer-arith -Wswitch-enum
HEADS=hash64.h hash32.h random64.h address.hpp sockets.hpp merdy_operations.h debug_mode.h dynamo_objects.hpp mercury_objects.hpp

target:master
target:worker
#target:proxy
target:mercury_test

master:master.o tcp_wrap.o
	$(CC) master.o tcp_wrap.o -o master $(OPTS) $(WARNS) $(LD)
worker:worker.o tcp_wrap.o
	$(CC) worker.o tcp_wrap.o -o worker $(OPTS) $(WARNS) $(LD)
proxy:proxy.o tcp_wrap.o
	$(CC) proxy.o tcp_wrap.o -o proxy $(OPTS) $(WARNS) $(LD)

mercury_test:mercury_test.o tcp_wrap.o
	$(CC) mercury_test.o tcp_wrap.o -o mercury_test  $(OPTS) $(WARNS) $(LD)


master.o:master.cpp tcp_wrap.o $(HEADS)
	$(CC) master.cpp -o master.o -c $(OPTS) $(WARNS)
worker.o:worker.cpp tcp_wrap.o $(HEADS)
	$(CC) worker.cpp -o worker.o -c $(OPTS) $(WARNS)
proxy.o:proxy.cpp tcp_wrap.o $(HEADS)
	$(CC) proxy.cpp -o proxy.o -c $(OPTS) $(WARNS)

mercury_test.o:mercury_test.cpp tcp_wrap.o $(HEADS)
	$(CC) mercury_test.cpp -o mercury_test.o -c $(OPTS) $(WARNS)

tcp_wrap.o:tcp_wrap.cpp tcp_wrap.h
	$(CC) tcp_wrap.cpp -o tcp_wrap.o -c $(OPTS) $(WARNS)