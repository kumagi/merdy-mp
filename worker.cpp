#include <stdlib.h>
#include <mp/wavy.h>
#include <mp/sync.h>
#include <unordered_set>
#include <msgpack.hpp>
#include "hash64.h"
#include "hash32.h"
#include "random64.h"
#include "tcp_wrap.h"
#include "address.hpp"
#include "sockets.hpp"
#include "merdy_operations.h"
#include <limits.h>
#include "debug_mode.h"


#include <boost/tuple/tuple.hpp>
#include <boost/program_options.hpp>
#include <unordered_map>

static const char interrupt[] = {-1,-12,-1,-3,6};

static struct settings{
	int verbose;
	unsigned short myport,masterport;
	int myip,masterip;
	int i_am_master;
	settings():verbose(10),myport(11011),masterport(11011),myip(get_myip()),masterip(aton("127.0.0.1")),i_am_master(1){}
}settings;

std::set<address> dynamo_nodes;
std::set<address> merdy_nodes;
std::map<uint64_t,address> dy_hash;
socket_set sockets;

// wait fowarding list
class fwd_wait{
public:
	const address target; // sended
	const std::string ident; // forward identifier
	
	fwd_wait(const address& _target, const std::string& _ident)
			:target(_target),ident(_ident){}
	bool operator==(const fwd_wait& rhs)const{
		return target == rhs.target && ident == rhs.ident;
	}
	unsigned int size()const{
		return sizeof(address) + ident.length();
	}
	void dump(void)const{
		target.dump();
		fprintf(stderr,"#%s#\n",ident.c_str());
	}
};
class fwd_hash{
public:
	std::size_t operator()(const fwd_wait& o)const{
		return o.target.hash32() + hash32(o.ident);
	}
};

namespace DY{
enum dynamo_param{
	// R + W > N 
	NUM = 3,
	READ = 2,
	WRITE = 2,
};
}
 

class value_vclock{
	std::string value;
	unsigned int clock;
public:
	value_vclock():value(""),clock(0){}
	value_vclock(const std::string& _value, int _clock=1):value(_value),clock(_clock){}
	value_vclock(const value_vclock& org):value(org.value),clock(org.clock){}
	
	int update(const std::string& _value, unsigned int _clock){
		if(clock < _clock){
			value = _value;
			clock = _clock;
			return 1;
		}else if(clock == _clock){
			return 0;
		}else{
			return -1;
		}
	}
	int update(const value_vclock& newitem){
		return update(newitem.value, newitem.clock);
	}
	void update(const std::string& _value){
		value = _value;
		clock++;
		return;
	}
	unsigned int get_clock(void)const{
		return clock;
	}
	const char* c_str(void)const{
		return value.c_str();
	}
	const std::string& get_string(void)const{
		return value;
	}
	void dump(void)const{
		fprintf(stderr, "[%s]#%d ",value.c_str(),clock);
	}
	MSGPACK_DEFINE(value, clock); // serialize and deserialize ok
private:
	value_vclock& operator=(const value_vclock&);
};

class get_fwd_t{
	int counter;
	value_vclock value_vc;
	const int origin;
public:
	get_fwd_t(const value_vclock& _value_vc, const int _origin):counter(0),value_vc(_value_vc),origin(_origin){}
	bool update(const value_vclock& newitem){
		++counter;
 		return value_vc.update(newitem);
	}
	inline bool count_gt(const int reads)const{
		return counter > reads;
	}
	inline int get_fd(void)const{
		return origin;
	}
	inline const std::string& get_value(void)const{
		return value_vc.get_string();
	}
	inline const value_vclock& get_vcvalue(void)const{
		return value_vc;
	}
	inline void dump(void)const{
		fprintf(stderr,"origin:%d ",origin);
		value_vc.dump();
		fprintf(stderr," cnt:%d ",counter);
	}
};

// mercury object

std::string get_strhalf(std::string begin, std::string end){
	int conflict = -1;
	unsigned char head,tail;
	do{
		conflict++;
		head = begin.c_str()[conflict];
		tail = end.c_str()[conflict];
	}while((char)((int)head + (int)tail / 2) == begin.c_str()[conflict]);
	char middle[begin.length()];
	memcpy(middle, begin.data(), begin.length());
	middle[conflict] = (char)((int)head + (int)tail / 2);
	middle[conflict+1] = '\0';
	return std::string(middle);

}


class attr{
	int num;
	std::string str;
	int num_or_str;
	enum flag{
		number = 0,
		string = 1,
		invalid = 2,
	};
public:
	attr():num(),str(),num_or_str(invalid){};
	attr(int _num):num(_num),str(),num_or_str(number){};
	attr(const std::string& _str):num(),str(_str),num_or_str(string){}
	attr(const attr& org):num(org.num),str(org.str),num_or_str(org.num_or_str){}
	
	int get_int()const{
		assert(num_or_str == number);
		return num;
	}
	std::string get_string() const{
		assert(num_or_str == string);
		return str;
	}
	void set_string(const std::string& _str){
		assert(num_or_str == string);
		str = _str;
	}
	bool is_int()const{
		return num_or_str == number;
	}
	bool is_invalid()const{
		return number == invalid;
	} 
	void set(int _num){
		num_or_str = number;
		num = _num;
	}
	void set(std::string& _str){
		num_or_str = string;
		str = _str;
	}
	attr get_middle(const attr& rhs)const{
		// assertion: rhs is bigger than me
		if(is_invalid()){
			assert(!rhs.is_invalid());
			if(rhs.is_int()){
				return attr(rhs.num + INT_MIN / 2);
			}else{
				return attr(get_strhalf(" ",rhs.get_string()));
			}
		}else if(rhs.is_invalid()){
			if(is_int()){
				return attr(num + INT_MAX / 2);
			}else{
				return attr(get_strhalf(get_string(),"~~~~~~~~~~~~~~"));
			}
		}else if(is_int()){
			return attr(num + rhs.num / 2);
		}else{
			return attr(get_strhalf(str, rhs.get_string()));
		}
	}
	bool operator<(const attr& rhs)const{
		assert(num_or_str == rhs.num_or_str);
		if(num_or_str == number){
			return num < rhs.num;
		}else if(num_or_str == string){
			return str < rhs.str;
		}else{
			assert(!"invalid comparison");
			return 0;
		}
	}
	bool operator==(const attr& rhs)const{
		assert(num_or_str == rhs.num_or_str);
		if(num_or_str == number){
			return num == rhs.num;
		}else if(num_or_str == string){
			return str == rhs.str;
		}else{
			assert(!"invalid comparison");
			return 0;
		}
	}
	void operator=(const attr& rhs){
		num = rhs.num;
		str = rhs.str;
		num_or_str = rhs.num_or_str;
	}
	MSGPACK_DEFINE(num, str, num_or_str); // serialize and deserialize ok
};



namespace closed{
enum flag{
	closed = true,
	opened = false,
};
}
class attr_range{
	attr begin;
	attr end;
	bool left_closed;
	bool right_closed;
public:
	attr_range():begin(),end(),left_closed(true),right_closed(true){};
	attr_range(const attr_range& org):begin(org.begin),end(org.end),left_closed(org.left_closed),right_closed(org.right_closed){};
	attr_range(const attr& _begin, const attr& _end):begin(_begin),end(_end),left_closed(true),right_closed(true){
		assert(_begin < _end);
	}
	attr_range(const attr& _begin, const attr& _end, const bool _l, const bool _r):begin(_begin),end(_end),left_closed(_l),right_closed(_r){
		assert(_begin < _end);
	}
	bool operator==(const attr_range& rhs)const{
		return begin == rhs.begin && end == rhs.end && left_closed == rhs.left_closed && right_closed == rhs.right_closed;
	}
	void get_up_from(const attr& border, attr_range* half)const{
		assert(is_inRange(border));
		assert(border != begin);
		half->begin = border;
		half->end = end;
		half->left_closed = true;
		half->right_closed = right_closed;
	}
	void get_down_from(const attr& border, attr_range* half)const{
		assert(is_inRange(border));
		assert(border != end);
		half->begin = begin;
		half->end = border;
		half->left_closed = left_closed;
		half->right_closed = false;
	}
	void cut_up_from(const attr& border, attr_range* half){
		assert(is_inRange(border));
		assert(border != begin);
		half->begin = border;
		half->end = end;
		half->left_closed = true;
		half->right_closed = right_closed;
		right_closed = false;
		end = border;
	}
	void cut_down_from(const attr& border, attr_range* half){
		assert(is_inRange(border));
		assert(border != end);
		half->begin = begin;
		half->end = border;
		half->left_closed = left_closed;
		half->right_closed = false;
		left_closed = true;
		begin = border;
	}
	void cut_uphalf(attr_range* half){
		attr middle = begin.get_middle(end);
		half->begin = middle;
		half->end = end;
		assert(middle != begin);
		half->left_closed = true;
		half->right_closed = right_closed;
		right_closed = false;
		end = middle;
	}
	void cut_downhalf(attr_range* half){
		attr middle = begin.get_middle(end);
		half->begin = begin;
		half->end = middle;
		assert(middle != end);
		half->left_closed = left_closed;
		half->right_closed = false;
		left_closed = true;
		begin = middle;
	}
	bool is_inRange(const attr& atr){
		if(begin.is_invalid()){
			return atr < end || (left_closed && atr == end);
		}else if(end.is_invalid()){
			return begin < atr || (right_closed && atr == begin);
		}else{
			return (begin < atr && atr < end) ||
				(left_closed && atr == begin) ||
				(right_closed && atr == end);
		}
	}
	bool operator<(const attr_range& rhs)const{
		return begin < rhs.begin;
	}
	const attr& get_begin()const{
		return begin;
	}
	
	MSGPACK_DEFINE(begin,end,left_closed,right_closed);
};

class mercury_instance{
	std::string attr_name;
	attr_range range;
	std::map<attr_range, address> hubs;
public:
	std::map<attr, int> kvp;
	mercury_instance():attr_name(""),range(){}
	mercury_instance(const std::string _name, const attr_range& _range):attr_name(_name),range(_range){}
	mercury_instance(const mercury_instance& org):attr_name(org.attr_name),range(org.range){}
	
	const std::string& get_name()const{
		return attr_name;
	}
	const attr_range& get_range()const{
		return range;
	}
	const std::map<attr_range, address>& get_hubs()const{
		return hubs;
	}
	
	bool operator<(const mercury_instance& rhs)const{
		return attr_name < rhs.attr_name;
	}
	bool operator==(const mercury_instance& rhs)const{
		return attr_name == rhs.attr_name;
	}
	
	MSGPACK_DEFINE(attr_name,range);
};

mp::sync< std::unordered_multimap<fwd_wait, int, fwd_hash> > set_fwd; // set, fd
mp::sync< std::unordered_multimap<fwd_wait, address,fwd_hash> > coordinate_fwd; // coordinate flag
mp::sync< std::unordered_multimap<std::string, std::pair<int,address> > > put_fwd; // counter and origin address
mp::sync< std::unordered_multimap<std::string, get_fwd_t > > send_fwd; // counter and value with origin address

mp::sync< std::unordered_multimap<std::string, mercury_instance> > mer_node;

std::unordered_map<std::string, value_vclock> key_value;

void dump_hashes(){
	std::map<uint64_t,address>::iterator it = dy_hash.begin();
	while(it != dy_hash.end()){
		it->second.dump();
		it++;
	}
}

template<typename tuple>
inline void tuple_send(const tuple& t, const address& ad){
	msgpack::vrefbuffer vbuf;
	msgpack::pack(vbuf, t);
	const struct iovec* iov(vbuf.vector());
	sockets.writev(ad, iov, vbuf.vector_size());
}

class main_handler : public mp::wavy::handler {
	mp::wavy::loop* lo;
	msgpack::unpacker m_pac;
	
	pthread_mutex_t mut;
	class lock_mut{
		pthread_mutex_t* mutex;
	public:
		lock_mut(pthread_mutex_t* _mutex):mutex(_mutex){
			pthread_mutex_lock(mutex);
		}
		~lock_mut(){
			pthread_mutex_unlock(mutex);
		}
	};
public:
	main_handler(int _fd,mp::wavy::loop* _lo):mp::wavy::handler(_fd),lo(_lo){
		pthread_mutex_init(&mut,NULL);
	}
	
	void event_handle(int fd, msgpack::object obj, msgpack::zone* z){
		msgpack::type::tuple<int> out(obj);
		int operation = out.get<0>();
		switch (operation){
			/* ---------------------- */
			// Dynamo operations
			/* ---------------------- */
		case OP::UPDATE_HASHES:{
			DEBUG_OUT("UPDATE_HASHES:");
			msgpack::type::tuple<int,std::map<uint64_t,address> > out(obj);
			std::map<uint64_t,address> tmp_dy_hash = out.get<1>();
			std::map<uint64_t,address>::iterator it = tmp_dy_hash.begin();
			while(it != tmp_dy_hash.end()){
				dy_hash.insert(*it);
				++it;
			}
			break;
		}
		case OP::OK_ADD_ME_DY:{
			DEBUG_OUT("OK_ADD_ME_DY");
			fprintf(stderr,"dynamo status: ok");
			break;
		}
		case OP::SET_DY:{// op, key, value
			DEBUG_OUT("SET_DY:");
			// search coordinator, and forward
			msgpack::type::tuple<int, std::string, std::string> set_dy(obj);
			const std::string& key = set_dy.get<1>();
			const std::string& value = set_dy.get<2>();
			DEBUG_OUT("key[%s] value[%s]",key.c_str(),value.c_str());
			
			uint64_t hash = hash64(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			if(it == dy_hash.end()){
				it = dy_hash.begin();
			}
			const address& coordinator = it->second;
			DEBUG(it->second.dump());
			
			mp::sync< std::unordered_multimap<fwd_wait, int, fwd_hash> >::ref set_fwd_r(set_fwd);
			set_fwd_r->insert(std::pair<fwd_wait,int>(fwd_wait(coordinator,key),fd));
			
			msgpack::type::tuple<int, std::string, std::string, address> 
				set_coordinate((int)OP::SET_COORDINATE, key, value, address(settings.myip,settings.myport));
			tuple_send(set_coordinate,coordinator);
			
			DEBUG_OUT("\n");
			break;
		}
		case OP::OK_SET_DY:{// op, key, address
			DEBUG_OUT("OK_SET_DY:");
			// responce for fowarding
			msgpack::type::tuple<int, std::string, address> ok_set_dy(obj);
			const std::string& key = ok_set_dy.get<1>();
			const address& org = ok_set_dy.get<2>();
			
			DEBUG(fwd_wait(org,key).dump());
			mp::sync< std::unordered_multimap<fwd_wait, int, fwd_hash> >::ref set_fwd_r(set_fwd);
			std::unordered_multimap<fwd_wait,int>::iterator it = set_fwd_r->find(fwd_wait(org,key));
			if(it == set_fwd_r->end()){
				fwd_wait(org, key).dump();
				fwd_wait(org, key).dump();
				DEBUG_OUT("not found\n");
				break;
			}
			std::string mes("ok");
			int fd = it->second;
			write(fd, mes.data(), 2);
			break;
		}
		case OP::SET_COORDINATE:{// op, key, value, address
			DEBUG_OUT("SET_COORDINATE:");
			msgpack::type::tuple<int, std::string, std::string, address> out(obj);
			const std::string& key = out.get<1>();
			const std::string& value = out.get<2>();
			const address& org = out.get<3>();
			mp::sync< std::unordered_multimap<std::string, std::pair<int,address> > >::ref put_fwd_r(put_fwd);
			put_fwd_r->insert(std::pair<std::string, std::pair<int, address> >(key, std::pair<int, address>(0,org)));
			
			uint64_t hash = hash64(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			if(it == dy_hash.end()){
				it = dy_hash.begin(); // hashtable is ring
			}
			
			value_vclock vcvalue;
			std::unordered_map<std::string, value_vclock>::iterator result = key_value.find(key);
			if(result == key_value.end()){
				vcvalue.update(value);
			}else{
				vcvalue.update(value,result->second.get_clock());
			}
			key_value.insert(std::pair<std::string, value_vclock>(key,vcvalue));
			++it;
			if(it == dy_hash.end()){
				it = dy_hash.begin();
			}
			
			for(int i=DY::NUM; i>0; --i){
				const address& target = it->second;
				DEBUG(target.dump());
				msgpack::type::tuple<int, std::string, value_vclock, address> put_dy((int)OP::PUT_DY, key, vcvalue, address(settings.myip,settings.myport));
				tuple_send(put_dy, target);
				++it;
				if(it == dy_hash.end()){
					it = dy_hash.begin();
				}
			}
			DEBUG_OUT("\n");
			break;
		}
		case OP::PUT_DY:{// op, key, value, origin address
			DEBUG_OUT("PUT_DY:");
			// store data, only coordinator can send this message
			msgpack::type::tuple<int, std::string, value_vclock, address> put_dy(obj);
			const std::string& key = put_dy.get<1>();
			const value_vclock& value = put_dy.get<2>();
			const address& ad = put_dy.get<3>();

			std::unordered_map<std::string, value_vclock>::iterator it = key_value.find(key);
			if(it == key_value.end()){
				// new insert
				key_value.insert(std::pair<std::string, value_vclock>(key, value_vclock(value)));
			}else{
				it->second.update(value);
			}
			DEBUG_OUT("saved:%s->%s\n",key.c_str(),value.c_str());
			
			msgpack::type::tuple<int, std::string> msg((int)OP::OK_PUT_DY, key);
			tuple_send(msg,ad);
			break;
		}
		case OP::OK_PUT_DY:{// op, key
			DEBUG_OUT("OK_PUT_DY:");
			// ack for PUT_DY, only coordinator should receives it
			msgpack::type::tuple<int, std::string> out(obj);
			const std::string& key = out.get<1>();
			mp::sync< std::unordered_multimap<std::string, std::pair<int,address> > >::ref put_fwd_r(put_fwd);
			std::unordered_multimap<std::string, std::pair<int, address> >::iterator it = put_fwd_r->find(key);
			if(it == put_fwd_r->end()){
				break;
			}
			++(it->second.first);
			if(it->second.first == DY::WRITE){
				// write ok
				DEBUG_OUT("write ok:[%s]\n",key.c_str());
				msgpack::type::tuple<int, std::string, address> ok_set((int)OP::OK_SET_DY, key, address(settings.myip,settings.myport));
				tuple_send(ok_set,it->second.second);
			}else if(it->second.first == DY::NUM){
				put_fwd_r->erase(it);
				DEBUG_OUT("erased\n");
			}else{
				DEBUG_OUT("ok %d\n",it->second.first);
			}
			break;
		}
		case OP::GET_DY:{
			DEBUG_OUT("GET_DY:");
			msgpack::type::tuple<int, std::string> get_dy(obj);
			const std::string& key = get_dy.get<1>();
			
			uint64_t hash = hash64(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			if(it == dy_hash.end()){
				it = dy_hash.begin();
			}
			mp::sync< std::unordered_multimap<std::string, get_fwd_t > >::ref send_fwd_r(send_fwd);
			send_fwd_r->insert(std::pair<std::string, get_fwd_t>(key, get_fwd_t(value_vclock(),fd)));
			
			for(int i=DY::NUM; i>0; --i){
				msgpack::type::tuple<int, std::string, address> send_dy((int)OP::SEND_DY, key, address(settings.myip,settings.myport));
				tuple_send(send_dy, it->second);
				++it;
				if(it == dy_hash.end()){
					it = dy_hash.begin();
				}
			}
			
			DEBUG_OUT("key:%s\n",key.c_str());
			break;
		}
		case OP::SEND_DY:{
			DEBUG_OUT("SEND_DY:");
			msgpack::type::tuple<int, std::string, address> send_dy(obj);
			const std::string& key = send_dy.get<1>();
			const address& org = send_dy.get<2>();
			
			std::unordered_multimap<std::string, value_vclock>::const_iterator ans
				= key_value.find(key);
			if(ans == key_value.end()){
				msgpack::type::tuple<int, std::string, address> notfound_dy((int)OP::NOTFOUND_DY, key, address(settings.myip,settings.myport));
				tuple_send(notfound_dy, org);
			}else{
				msgpack::type::tuple<int, std::string, value_vclock, address> found_dy((int)OP::FOUND_DY, key, ans->second, address(settings.myip,settings.myport));
				tuple_send(found_dy, org);
				DEBUG_OUT("found ");
				DEBUG(ans->second.dump());
				DEBUG_OUT("\n");
			}
			break;
		}
		case OP::FOUND_DY:{
			DEBUG_OUT("FOUND_DY:");
			msgpack::type::tuple<int, std::string, value_vclock, address> found_dy(obj);
			const std::string& key = found_dy.get<1>();
			const value_vclock& value = found_dy.get<2>();
			const address& org = found_dy.get<3>();
			
			mp::sync< std::unordered_multimap<std::string, get_fwd_t > >::ref send_fwd_r(send_fwd);
			std::unordered_multimap<std::string, get_fwd_t>::iterator it = send_fwd_r->find(key);
			if(it == send_fwd_r->end()){
				DEBUG_OUT("%s already answered\n", key.c_str());
				
				for(std::unordered_multimap<std::string, get_fwd_t>::iterator it=send_fwd_r->begin();it != send_fwd_r->end(); ++it){
					fprintf(stderr,"key[%s],",it->first.c_str());
				}
				break;
			}
			int result = it->second.update(value);
			if(result == -1){ // old data found -> read repair
				msgpack::type::tuple<int, std::string, value_vclock, address> 
					put_dy((int)OP::PUT_DY, key, it->second.get_vcvalue(), address(settings.myip,settings.myport));
				tuple_send(put_dy, org);
			}
			if(it->second.count_gt(DY::READ-1)){
				int fd = it->second.get_fd();
				std::string answer("");
				answer += "found ";
				answer += key;
				answer += " -> ";
				answer += it->second.get_value();
				lo->write(fd ,answer.c_str(), answer.length());
				
				std::unordered_multimap<std::string, value_vclock>::iterator ans
					= key_value.find(key);
				ans->second.update(it->second.get_value());
				
				DEBUG(it->second.dump());
				
				send_fwd_r->erase(it);
				DEBUG_OUT("answered ok\n");
			}else{
				DEBUG_OUT("updated ");
				DEBUG(it->second.dump());
				DEBUG_OUT("\n");
			}
			break;
		}
		case OP::NOTFOUND_DY:{
			DEBUG_OUT("NOTFOUND_DY:");
			msgpack::type::tuple<int, std::string, address> notfound_dy(obj);
			const std::string& key = notfound_dy.get<1>();
			const address& org = notfound_dy.get<2>();
			
			mp::sync< std::unordered_multimap<std::string, get_fwd_t > >::ref send_fwd_r(send_fwd);
			std::unordered_multimap<std::string, get_fwd_t>::iterator it = send_fwd_r->find(key);
			
			msgpack::type::tuple<int, std::string, value_vclock, address> 
				put_dy((int)OP::PUT_DY, key, it->second.get_vcvalue(), address(settings.myip,settings.myport));
			tuple_send(put_dy, org);
			break;
		}
		case OP::DEL_DY:{
			break;
		}
			/* ---------------------- */
			// Mercury operations
			/* ---------------------- */
		case OP::OK_ADD_ME_MER:{
			DEBUG_OUT("OK_ADD_ME_MER");
			fprintf(stderr,"mercury status: ok");
			break;
		}
		case OP::UPDATE_MER_HUB:{// op, std::map<address>
			DEBUG_OUT("UPDATE_MER_HUB:");
			msgpack::type::tuple<int, std::set<address> > update_mer_hub(obj);
			std::set<address>& tmp_mer_hub = update_mer_hub.get<1>();
			std::set<address>::iterator it = tmp_mer_hub.begin();
			
			break;
		}
		case OP::ASSIGN_ATTR:{ // op, mercury_instance
			DEBUG_OUT("ASSIGN_ATTR");
			msgpack::type::tuple<int, mercury_instance> assign_attr(obj);
			mercury_instance& mi = assign_attr.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			mer_node_r->insert(std::pair<std::string, mercury_instance>(mi.get_name(),mi));
			break;
		}
		case OP::SET_ATTR:{ // op, attr_name, attr, int
			break;
		}
		case OP::OK_SET_ATTR:{
			break;
		}
		case OP::GET_RANGE:{ // op, attr_name, range, org_addres
			DEBUG_OUT("GET_RANGE");
			const msgpack::type::tuple<int, std::string, attr_range, address> get_range(obj);
			const std::string& name = get_range.get<1>();
			const attr_range& range = get_range.get<2>();
			const address& org = get_range.get<3>();
			
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it_mi = mer_node_r->find(name);
			
			std::map<attr, int>& key_value = it_mi->second.kvp;
			const attr_range& myrange = it_mi->second.get_range();
			const std::map<attr_range, address>& hub = it_mi->second.get_hubs();
			if(range.get_begin() < it_mi->second.get_range().get_begin()){
				std::map<attr_range, address>::const_iterator i = hub.begin();
				if(i->first)
				attr_range lower;
				range.get_down_from(myrange.get_begin(), &lower);
			}
			std::map<attr, int>::iterator it = key_value.lower_bound(range.get_begin());
			
			
			break;
		}
		case OP::TELLME_RANGE:{ // op, attrname, address
			const msgpack::type::tuple<int, std::string, address> tellme_range(obj);
			const std::string& name = tellme_range.get<1>();
			const address& org = tellme_range.get<2>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			if(it != mer_node_r->end()){
				msgpack::type::tuple<int, std::string, attr_range> ok_tellme_range(OP::OK_TELLME_RANGE, name, it->second.get_range());
				tuple_send(ok_tellme_range, org);
			}else{
				msgpack::type::tuple<int, std::string> ng_tellme_range(OP::NG_TELLME_RANGE, name);
				tuple_send(ng_tellme_range, org);
			}
			break;
		}
		case OP::OK_TELLME_RANGE:{
			msgpack::type::tuple<int, std::string> ok_tellme_range(obj);
			std::string& name = ok_tellme_range.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			
			break;
		}
		case OP::NG_TELLME_RANGE:{
			msgpack::type::tuple<int, std::string> ng_tellme_range(obj);
			std::string& name = ng_tellme_range.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			break;
		}
		case OP::GIVEME_RANGE:{
			break;
		}
		case OP::ASSIGN_RANGE:{
			break;
		}
		case OP::OK_ASSIGN_RANGE:{
			break;
		}
		case OP::DEL_RANGE:{
			break;
		}
		
		}
	}
	void on_read(mp::wavy::event& e)
	{
		try{
			while(true) {
				if(m_pac.execute()) {
					msgpack::object msg = m_pac.data();
					std::shared_ptr<msgpack::zone> z( m_pac.release_zone() );
					m_pac.reset();

					e.more();  //e.next();
					
					DEBUG(std::cout << "object received: " << msg << "->");
					
					DEBUG(lock_mut lock(&mut););
					event_handle(fd(), msg, &*z);
					return;
				}
			
				m_pac.reserve_buffer(8*1024);

				int read_len = ::read(fd(), m_pac.buffer(), m_pac.buffer_capacity());
				if(read_len <= 0) {
					if(read_len == 0) {throw mp::system_error(errno, "connection closed"); }
					if(errno == EAGAIN || errno == EINTR) { return; }
					else { perror("read"); throw mp::system_error(errno, "read error"); }
				}
				m_pac.buffer_consumed(read_len);
			}
		}catch(...){
			perror("exception ");
			e.remove();
		}
	}
};

void on_accepted(int fd, int err, mp::wavy::loop* lo)
{
	fprintf(stderr,"accept %d %d\n",fd,err);
	if(fd < 0) {
		perror("accept error");
		exit(1);
	}

	try {
		int on = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
		std::tr1::shared_ptr<main_handler> p = lo->add_handler<main_handler>(fd,lo);
		
	} catch (...) {
		fprintf(stderr,"listening socket error");
		::close(fd);
		return;
	}
}

namespace po = boost::program_options;
int main(int argc, char** argv){
	srand(time(NULL));
	
	// parse options
	po::options_description opt("options");
	std::string master;
	opt.add_options()
		("help,h", "view help")
		("verbose,v", "verbose mode")
		("port,p",po::value<unsigned short>(&settings.myport)->default_value(11011), "my port number")
		("address,a",po::value<std::string>(&master)->default_value("127.0.0.1"), "master's address")
		("mport,P",po::value<unsigned short>(&settings.masterport)->default_value(11011), "master's port");
	

	po::variables_map vm;
	store(parse_command_line(argc,argv,opt), vm);
	notify(vm);
	if(vm.count("help")){
		std::cout << opt << std::endl;
		return 0;
	}
	
	// set options
	if(vm.count("verbose")){
		settings.verbose++;
	}
	settings.masterip = aton(master.c_str());
	if(settings.masterip != aton("127.0.0.1") || settings.myport != settings.masterport){
		settings.i_am_master = 0;
	}
	
	// view options
	printf("verbose:%d\naddress:[%s:%d]\n",
		   settings.verbose,ntoa(settings.myip),settings.myport);
	printf("master:[%s:%d] %s\n"
		   ,ntoa(settings.masterip),settings.masterport,settings.i_am_master?"self!":"");
	
	address my_address(settings.myip,settings.myport);
	
	
	// init mpio
	mp::wavy::loop lo;
	sockets.set_wavy_loop(&lo);
	
    if (sigignore(SIGPIPE) == -1) {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(1);
    }
	
	using namespace mp::placeholders;
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_port = htons(settings.myport);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	lo.listen(PF_INET, SOCK_STREAM, 0,
			  (struct sockaddr*)&addr, sizeof(addr),
			  mp::bind(&on_accepted, _1, _2, &lo)); 

	// hello master.
	{
		int masterfd = create_tcpsocket();
		connect_ip_port(masterfd, settings.masterip,settings.masterport);
		
		int answer = OP::ADD_ME_DY;
		address myaddress = address(settings.myip,settings.myport);
	
		msgpack::type::tuple<int,address> mes(answer, myaddress);
		
		msgpack::vrefbuffer vbuf;
		msgpack::pack(vbuf,mes);
		const struct iovec* iov(vbuf.vector());
		
		writev(masterfd,iov, vbuf.vector_size());
	}
	
	// mpio start
	lo.run(2);
} 
