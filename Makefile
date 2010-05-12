CC=g++44
OPTS=-Wall -W -fexceptions -std=c++0x  -march=x86-64 -g
LD=-lmpio -lmsgpack -lmsgpackc -pthread -lboost_program_options
WARNS = -Wextra -Wformat=2 -Wstrict-aliasing=2 -Wcast-qual -Wcast-align \
	-Wwrite-strings -Wfloat-equal -Wpointer-arith -Wswitch-enum -Winline
OBJS=tcp_wrap.o

target:master
target:worker
target:client

master:master.o tcp_wrap.o
	$(CC) master.o $(OBJS) -o master $(OPTS) $(WARNS) $(LD)
worker:worker.o tcp_wrap.o
	$(CC) worker.o $(OBJS) -o worker $(OPTS) $(WARNS) $(LD)
client:client.o tcp_wrap.o
	$(CC) client.o $(OBJS) -o client $(OPTS) $(WARNS) $(LD)

master.o:master.cpp
	$(CC) master.cpp -o master.o -c $(OPTS) $(WARNS)
worker.o:worker.cpp
	$(CC) worker.cpp -o worker.o -c $(OPTS) $(WARNS)
client.o:client.cpp
	$(CC) client.cpp -o client.o -c $(OPTS) $(WARNS)


tcp_wrap.o:tcp_wrap.cpp tcp_wrap.h
	$(CC) tcp_wrap.cpp -o tcp_wrap.o -c $(OPTS) $(WARNS)