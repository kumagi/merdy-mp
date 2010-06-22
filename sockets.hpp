#include "address.hpp"
#include <mp/wavy.h>
#include <mp/sync.h>
#include <msgpack.h>
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
	inline void writev(const address& ad ,const struct iovec* const vec, size_t veclen){
		int fd = get_socket(ad);
		
		int result = ::writev(fd, vec,veclen);
		if(result < 1){
            perror("writev");
			close(fd);
			fprintf(stderr,"reconnect: ");
			ad.dump();
			fprintf(stderr,"\n");
			remove(fd);
			int newfd = get_socket(ad);
			int result = ::writev(newfd, vec, veclen);
			if(result<1){
				assert(!"cannot connect");
			}
		}else{
			//fprintf(stderr,"%d:%d byte\n",fd,result);
		}
	}

	template<class T>
	inline void writev(const address& ad ,const iovec* const vec, size_t veclen, mp::shared_ptr<T>& zone){
		int fd = get_socket(ad);
		lo->writev(fd, vec,veclen,zone);
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
	inline bool remove(const int fd){
		mp::sync< std::unordered_map<address, int,address_hash> >::ref fds_r(fds);
		std::unordered_map<address,int>::const_iterator it = fds_r->begin();
		while(it != fds_r->end()){
			if(it->second == fd){
				fds_r->erase(it);
				break;
			}
			++it;
		}
		return it != fds_r->end();
	}
	inline void remove(const address& ad){
		mp::sync< std::unordered_map<address, int,address_hash> >::ref fds_r(fds);
		std::unordered_map<address,int>::const_iterator it = fds_r->find(ad);
		fds_r->erase(ad);
	}
		
};

template<typename tuple>
inline void tuple_send(const tuple& t, const address& ad, socket_set* s){
	msgpack::vrefbuffer vbuf;
	msgpack::pack(vbuf, t);
	const struct iovec* iov(vbuf.vector());
	s->writev(ad, iov, vbuf.vector_size());
}

template<typename tuple>
inline void tuple_send_async(const tuple* t, const address& ad, socket_set* s, mp::shared_ptr<msgpack::zone> z){
	msgpack::vrefbuffer* vbuf = z->allocate<msgpack::vrefbuffer>();
	msgpack::pack(*vbuf, *t);
	const struct iovec* iov(vbuf->vector());
	s->writev(ad, iov, vbuf->vector_size(),z);
}
