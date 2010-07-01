CC=g++
OPTS=-O4 -fexceptions -std=c++0x -march=x86-64 -g
LD=-lmpio -lmsgpack -pthread -lboost_program_options  -ltokyocabinet
WARNS= -W -Wall -Wextra -Wformat=2 -Wstrict-aliasing=4 -Wcast-qual -Wcast-align \
	-Wwrite-strings -Wfloat-equal -Wpointer-arith -Wswitch-enum
HEADS=hash64.h hash32.h random64.h address.hpp sockets.hpp merdy_operations.h debug_mode.h dynamo_objects.hpp mercury_objects.hpp 

target:master
target:worker
target:proxy
target:client
target:random
target:dynamo_test
#target:mercury_test

master:master.o tcp_wrap.o
	$(CC) master.o tcp_wrap.o -o master $(OPTS) $(WARNS) $(LD)
worker:worker.o tcp_wrap.o
	$(CC) worker.o tcp_wrap.o -o worker $(OPTS) $(WARNS) $(LD)
proxy:proxy.o tcp_wrap.o
	$(CC) proxy.o tcp_wrap.o -o proxy $(OPTS) $(WARNS) $(LD)
client:client.o tcp_wrap.o
	$(CC) client.o tcp_wrap.o -o client $(OPTS) $(WARNS) $(LD)
random:random_client.o tcp_wrap.o
	$(CC) random_client.o tcp_wrap.o -o random $(OPTS) $(WARNS) $(LD)
main:main.o Apriori.o
	$(CC) main.o Apriori.o -o main $(OPTS) $(WARNS) $(LD)

dynamo_test:dynamo_test.o tcp_wrap.o
	$(CC) dynamo_test.o tcp_wrap.o -o dynamo_test $(OPTS) $(WARNS) $(LD)
mercury_test:mercury_test.o tcp_wrap.o
	$(CC) mercury_test.o tcp_wrap.o -o mercury_test  $(OPTS) $(WARNS) $(LD)


master.o:master.cpp tcp_wrap.o $(HEADS)
	$(CC) master.cpp -o master.o -c $(OPTS) $(WARNS)
worker.o:worker.cpp tcp_wrap.o tcpp.hpp $(HEADS)
	$(CC) worker.cpp -o worker.o -c $(OPTS) $(WARNS)
proxy.o:proxy.cpp tcp_wrap.o $(HEADS) sqlparser.hpp
	$(CC) proxy.cpp -o proxy.o -c $(OPTS) $(WARNS)
client.o:client.cpp
	$(CC) client.cpp -o client.o -c $(OPTS) $(WARNS)
random_client.o:random_client.cpp 
	$(CC) random_client.cpp -o random_client.o -c $(OPTS) $(WARNS)
main.o:main.cpp
	$(CC) main.cpp -o main.o -c $(OPTS) $(WARNS)
Apriori.o:Apriori.cpp
	$(CC) Apriori.cpp -o Apriori.o -c  $(OPTS) $(WARNS)

dynamo_test.o:dynamo_test.cpp tcp_wrap.o $(HEADS)
	$(CC) dynamo_test.cpp -o dynamo_test.o -c $(OPTS) $(WARNS)
mercury_test.o:mercury_test.cpp tcp_wrap.o $(HEADS)
	$(CC) mercury_test.cpp -o mercury_test.o -c $(OPTS) $(WARNS)

tcp_wrap.o:tcp_wrap.cpp tcp_wrap.h
	$(CC) tcp_wrap.cpp -o tcp_wrap.o -c $(OPTS) $(WARNS)