#include "address.hpp"


class socket_set{
	mp::wavy::loop* lo;
	std::map<address,int> fds; // fixme: it shuould be searchable address and socket
public:
	socket_set():lo(NULL){}
	socket_set(mp::wavy::loop* _lo):lo(_lo){}
	void set_wavy_loop(mp::wavy::loop* _lo){
		lo = _lo;
	}
	void write(const address& ad ,const void* const buff,int size){
		int fd = get_socket(ad);
		lo->write(fd, buff,size);
	}
	void writev(const address& ad ,const struct iovec* vec, size_t veclen){
		int fd = get_socket(ad);
		::writev(fd, vec,veclen);
	}
private:
	inline int get_socket(const address& ad){
		std::map<address,int>::const_iterator it = fds.find(ad);
		if(it == fds.end()){
			int newfd = create_tcpsocket();
			connect_ip_port(newfd, ad.get_ip(), ad.get_port());
			fds.insert(std::pair<address,int>(ad,newfd));
			return newfd;
		}else{
			return it->second;
		}
	}
};
