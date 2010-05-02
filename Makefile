CC=g++44
OPTS=-Wall -W -fexceptions -std=c++0x  -march=x86-64 
LD=-lmpio -lmsgpack -lmsgpackc -pthread -lboost_program_options
WARNS = -Wextra -Wformat=2 -Wstrict-aliasing=2 -Wcast-qual -Wcast-align \
	-Wwrite-strings -Wfloat-equal -Wpointer-arith -Wswitch-enum
OBJS=tcp_wrap.o

target:master
target:worker

master:master.o tcp_wrap.o
	$(CC) master.o $(OBJS) -o master $(OPTS) $(WARNS) $(LD)
worker:worker.o tcp_wrap.o
	$(CC) worker.o $(OBJS) -o worker $(OPTS) $(WARNS) $(LD)

master.o:master.cpp
	$(CC) master.cpp -o master.o -c $(OPTS) $(WARNS)
worker.o:worker.cpp
	$(CC) worker.cpp -o worker.o -c $(OPTS) $(WARNS)



tcp_wrap.o:tcp_wrap.cpp tcp_wrap.h
	$(CC) tcp_wrap.cpp -o tcp_wrap.o -c $(OPTS) $(WARNS)