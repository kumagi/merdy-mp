#ifndef ADDRESS_H_
#define ADDRESS_H_
#include <boost/functional/hash.hpp>
#include <msgpack.hpp>
#include "hash32.h"

class address {
private:
	int ip;
	unsigned short port;
public:
	address():ip(aton("127.0.0.1")),port(11211){};
	address(const int _ip,const unsigned short _port):ip(_ip),port(_port){};
	address(const address& ad):ip(ad.ip),port(ad.port){}
	void dump(void)const{
		fprintf(stderr,"[%s:%d]", ntoa(ip),port);
	}
	int get_ip()const {return ip;}
	unsigned short get_port()const {return port;}
	
	unsigned int serialize(char* const ptr) const{
		int* int_ptr = (int*)ptr;
		*int_ptr = ip;
		unsigned short* short_ptr = (unsigned short*)(ptr+4);
		*short_ptr = port;
		return 6;
	}
	unsigned int deserialize(const char* const ptr){
		ip = *(int*)ptr;
		port = *(unsigned short*)(ptr+2);
		return 6;
	}
	unsigned int getLength()const {return 6;}
	bool operator==(const address& rhs)const {
		return ip == rhs.ip && port == rhs.port;
	}
	bool operator!=(const address& rhs)const{
		return ip != rhs.ip || port != rhs.port;
	}
	bool operator<(const address& rhs)const{
		if(ip < rhs.ip)return true;
		if(ip > rhs.ip)return false;
		if(port < rhs.port)return true;
		else return false;
	}
	size_t hash32(void)const{
		return ::hash32(this,6);
	}
	
	MSGPACK_DEFINE(ip, port); // serialize and deserialize ok
};

size_t hash(const address& ad){
	return ad.hash32();
}

class address_hash{
public:
	std::size_t operator()(const address& ad)const{
		return ad.hash32();
	}
};

#endif
