#include "address.hpp"
#include <mp/sync.h>
#include <unordered_map>

class socket_set{
	mp::wavy::loop* lo;
	mp::sync< std::unordered_map<address, int,address_hash> > fds;
	
public:
	socket_set():lo(NULL){
	}
	socket_set(mp::wavy::loop* _lo):lo(_lo){}
	void set_wavy_loop(mp::wavy::loop* _lo){
		lo = _lo;
	}
	inline void write(const address& ad ,const void* const buff,int size){
		int fd = get_socket(ad);
		lo->write(fd, buff,size);
	}
	inline void writev(const address& ad ,const struct iovec* vec, size_t veclen){
		int fd = get_socket(ad);
		::writev(fd, vec,veclen);
	}
	inline int get_socket(const address& ad){
		mp::sync< std::unordered_map<address, int,address_hash> >::ref fds_r(fds);
		std::unordered_map<address,int>::const_iterator it = fds_r->find(ad);
		if(it == fds_r->end()){
			int newfd = create_tcpsocket();
			connect_ip_port(newfd, ad.get_ip(), ad.get_port());
			fds_r->insert(std::pair<address,int>(ad,newfd));
			return newfd;
		}else{
			return it->second;
		}
	}
};
