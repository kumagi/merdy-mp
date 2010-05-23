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
#include "dynamo_objects.hpp"
#include "mercury_objects.hpp"


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


mp::sync< std::unordered_multimap<fwd_wait, int, fwd_hash> > set_fwd; // set, fd
mp::sync< std::unordered_multimap<fwd_wait, address,fwd_hash> > coordinate_fwd; // coordinate flag
mp::sync< std::unordered_multimap<std::string, std::pair<int,address> > > put_fwd; // counter and origin address
mp::sync< std::unordered_multimap<std::string, get_fwd_t > > send_fwd; // counter and value with origin address

mp::sync< std::unordered_multimap<std::string, mercury_instance> > mer_node;
mp::sync< std::unordered_multimap<range_query_fwd, mer_get_fwd, range_query_hash> > mer_range_fwd;

mp::sync< std::unordered_map<std::string, value_vclock> > key_value;

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
class main_handler : public mp::wavy::handler {
	mp::wavy::loop* lo;
	msgpack::unpacker m_pac;
	
public:
	main_handler(int _fd,mp::wavy::loop* _lo):mp::wavy::handler(_fd),lo(_lo){
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
			const msgpack::type::tuple<int,std::map<uint64_t,address> > out(obj);
			const std::map<uint64_t,address>& tmp_dy_hash = out.get<1>();
			std::map<uint64_t,address>::const_iterator it = tmp_dy_hash.begin();
			while(it != tmp_dy_hash.end()){
				dy_hash.insert(*it);
				++it;
			}
			DEBUG_OUT("%d hash received\n",(int)tmp_dy_hash.size());
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
			mp::sync< std::unordered_map<std::string, value_vclock> >::ref key_value_r(key_value);
			std::unordered_map<std::string, value_vclock>::iterator result = key_value_r->find(key);
			if(result == key_value_r->end()){
				vcvalue.update(value);
			}else{
				vcvalue.update(value,result->second.get_clock());
			}
			key_value_r->insert(std::pair<std::string, value_vclock>(key,vcvalue));
			++it;
			if(it == dy_hash.end()){
				it = dy_hash.begin();
			}
			
			int tablesize = dy_hash.size();
			std::unordered_set<address, address_hash> already_send;
			for(int i=DY::NUM; i>0; --i){
				const address& target = it->second;
				DEBUG(target.dump());
				if(already_send.find(target) == already_send.end() && target != address(settings.myip,settings.myport)){
					msgpack::type::tuple<int, std::string, value_vclock, address> put_dy((int)OP::PUT_DY, key, vcvalue, address(settings.myip,settings.myport));
					tuple_send(put_dy, target);
					
					already_send.insert(target);
				}else {
					++i;
				}
				++it;
				if(it == dy_hash.end()){
					it = dy_hash.begin();
				}
				
				--tablesize;
				if(tablesize == 0){
					break;
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
			mp::sync< std::unordered_map<std::string, value_vclock> >::ref key_value_r(key_value);
			std::unordered_map<std::string, value_vclock>::iterator it = key_value_r->find(key);
			if(it == key_value_r->end()){
				// new insert
				key_value_r->insert(std::pair<std::string, value_vclock>(key, value_vclock(value)));
				DEBUG_OUT("saved:%s->%s new!\n",key.c_str(),value_vclock(value).c_str());
			}else{
				DEBUG_OUT("saved:%s->",key.c_str());
				DEBUG(it->second.dump());
				DEBUG_OUT("   ");
				it->second.update(value.get_string());
				DEBUG_OUT("saved:%s->",key.c_str());
				DEBUG(it->second.dump());
				DEBUG_OUT("\n");
			}
			
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
			
			int tablerest = dy_hash.size();
			std::unordered_set<address, address_hash> sentlist;
			for(int i=DY::NUM; i>0; --i){
				if(sentlist.find(it->second) == sentlist.end()){
					msgpack::type::tuple<int, std::string, address> send_dy((int)OP::SEND_DY, key, address(settings.myip,settings.myport));
					tuple_send(send_dy, it->second);
					sentlist.insert(it->second);
				}else{++i;}
				++it;
				if(it == dy_hash.end()){
					it = dy_hash.begin();
				}
				
				--tablerest;
				if(tablerest == 0){
					break;
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
			
			mp::sync< std::unordered_map<std::string, value_vclock> >::ref key_value_r(key_value);
			std::unordered_multimap<std::string, value_vclock>::const_iterator ans
				= key_value_r->find(key);
			if(ans == key_value_r->end()){
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
		case OP::FOUND_DY:{ // op, key, vcvalue, address
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
			if(it->second.count_eq(DY::READ)){
				
				int fd = it->second.get_fd();
				std::string answer("");
				answer += "found ";
				answer += key;
				answer += " -> ";
				answer += it->second.get_value();
				lo->write(fd ,answer.c_str(), answer.length());
				
				DEBUG_OUT("counter:%d ",it->second.get_cnt());
				
				mp::sync< std::unordered_map<std::string, value_vclock> >::ref key_value_r(key_value);
				std::unordered_multimap<std::string, value_vclock>::iterator ans = key_value_r->find(key);

				if(ans != key_value_r->end()){
					ans->second.update(it->second.get_value());
				}else{
					key_value_r->insert(std::pair<std::string, value_vclock>(it->first,it->second.get_value()));
				}
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
			DEBUG_OUT("for %s\n",key.c_str());
			
			mp::sync< std::unordered_multimap<std::string, get_fwd_t > >::ref send_fwd_r(send_fwd);
			std::unordered_multimap<std::string, get_fwd_t>::iterator it = send_fwd_r->find(key);
			
			if(it != send_fwd_r->end()){
				// read repair
				msgpack::type::tuple<int, std::string, value_vclock, address> 
					put_dy((int)OP::PUT_DY, key, it->second.get_vcvalue(), address(settings.myip,settings.myport));
				tuple_send(put_dy, org);
			}
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
			fprintf(stderr," mercury status: ok\n");
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
			break;
		}
		case OP::SET_ATTR:{ // op, attr_name, list<mercury_kvp>, address
			DEBUG_OUT("SET_ATTR:");
			const msgpack::type::tuple<int, std::string, std::list<mercury_kvp>, address > set_attr(obj);
			const std::string& name = set_attr.get<1>();
			const std::list<mercury_kvp> data = set_attr.get<2>(); 
			const address& org = set_attr.get<3>();
			
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it_mi = mer_node_r->find(name);
			
			std::map<attr, uint64_t>& target_kvp = it_mi->second.kvp;
			std::list<mercury_kvp>::const_iterator it = data.begin();
			while(it != data.end()){
				if(it_mi->second.get_range().contain(it->id)){
					target_kvp.insert(std::pair<attr,int>(it->id,it->data));
				}else{// pass SET_ATTR to correct node
					std::map<attr_range, address>::const_iterator i = it_mi->second.get_hubs().begin();
					while(i != it_mi->second.get_hubs().end()){
						if(i->first.contain(it->id)){
							assert(i->second != address(settings.myip,settings.myport));
							std::list<mercury_kvp> fwd_data;
							fwd_data.push_back(mercury_kvp(it->id,it->data));
							const msgpack::type::tuple<int, std::string, std::list<mercury_kvp>, address > 
								set_attr(OP::SET_ATTR, name, fwd_data, address(settings.myip,settings.myport));
							tuple_send(set_attr, i->second);
							break;
						}
						++i;
					}
				}
				++it;
			}
			const msgpack::type::tuple<int, std::string> ok_set_attr(OP::OK_SET_ATTR, name);
			tuple_send(ok_set_attr, org);
			break;
		}
		case OP::OK_SET_ATTR:{ // op, attr_name
			DEBUG_OUT("OK_SET_ATTR:");
			
			break;
		}
		case OP::GET_RANGE:{ // op, attr_name, range, org_addres
			DEBUG_OUT("GET_RANGE:");
			const msgpack::type::tuple<int, std::string, attr_range, address, int> get_range(obj);
			const std::string& name = get_range.get<1>();
			const attr_range& range = get_range.get<2>();
			const address& org = get_range.get<3>();
			const int& id = get_range.get<4>();
			
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it_mi = mer_node_r->find(name);
			
			std::map<attr, uint64_t>& key_value = it_mi->second.kvp;
			const attr_range& myrange = it_mi->second.get_range();
			const std::map<attr_range, address>& hub = it_mi->second.get_hubs();
			
			std::list<mercury_kvp> myans;
			if(myrange.contain(range)){
				std::map<attr, uint64_t>::iterator it = key_value.lower_bound(range.get_begin());
				while(it != key_value.end() && range.contain(it->first)){
					myans.push_back(mercury_kvp(it->first,it->second));
					++it;
				}
			}
			
			bool forwarded = false;
			if(range.get_begin() < myrange.get_begin()){ // if out of my range
				forwarded = true;
				mp::sync< std::unordered_multimap<range_query_fwd,
					store_and_counter, range_query_hash> >::ref mer_fwd_r(mer_fwd);
				int counter = 0;
				std::map<attr_range, address>::const_iterator i = hub.begin();
				while(i != hub.end()){
					if(range.contain(i->first) && i->second != address(settings.myip,settings.myport)){
						attr_range lower;
						range.get_common_range(i->first, &lower);
						const address& target(i->second);
						msgpack::type::tuple<int, std::string, attr_range, address, int> get_range(OP::GET_RANGE, name, lower, address(settings.myip,settings.myport),id);
						tuple_send(get_range, target);
						counter++;
					}
					++i;
				}
				mer_fwd_r->insert(std::pair<range_query_fwd, store_and_counter>(range_query_fwd(id,name), store_and_counter(counter,myans,address(settings.myip,settings.myport))));
			}
			
			if(forwarded){
				
			}else{
				const msgpack::type::tuple<int, std::string, std::list<mercury_kvp>, address>
					ok_get_range(OP::OK_GET_RANGE, name, myans, address(settings.myip, settings.myport));
				tuple_send(ok_get_range, org);
			}
			break;
		}
		case OP::OK_GET_RANGE:{
			DEBUG_OUT("OK_GET_RANGE:");
			const msgpack::type::tuple<int, std::string, std::list<mercury_kvp>, int> ok_get_range(obj);
			const std::string& name = ok_get_range.get<1>();
			const std::list<mercury_kvp>& ans = ok_get_range.get<2>();
			const int& id = ok_get_range.get<3>();
			mp::sync< std::unordered_multimap<range_query_fwd,
					store_and_counter, range_query_hash> >::ref mer_fwd_r(mer_fwd);
			
			std::unordered_multimap<range_query_fwd, store_and_counter>::iterator it =  mer_fwd_r->find(range_query_fwd(id,name));
			it->second.merge(ans);
			if(it->second.is_complete()){
				const msgpack::type::tuple<int, std::string, std::list<mercury_kvp>, address>
					ok_get_range(OP::OK_GET_RANGE, name, it->second.store, address(settings.myip, settings.myport));
				tuple_send(ok_get_range, it->second.org);
			}
			break;
		}
		case OP::TELLME_RANGE:{ // op, attrname, address
			DEBUG_OUT("TELLME_RANGE:");
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
			DEBUG_OUT("OK_TELLME_RANGE:");
			msgpack::type::tuple<int, std::string> ok_tellme_range(obj);
			std::string& name = ok_tellme_range.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			
			break;
		}
		case OP::NG_TELLME_RANGE:{
			DEBUG_OUT("NG_TELLME_RANGE:");
			msgpack::type::tuple<int, std::string> ng_tellme_range(obj);
			std::string& name = ng_tellme_range.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			break;
		}
		case OP::GIVEME_RANGE:{
			DEBUG_OUT("GIVEME_RANGE:");
			break;
		}
		case OP::ASSIGN_RANGE:{// op, name, range
			DEBUG_OUT("ASSIGN_RANGE:");
			const msgpack::type::tuple<int, std::string, attr_range, address> assign_range(obj);
			const std::string& name = assign_range.get<1>();
			const attr_range& range = assign_range.get<2>();
			const address& org = assign_range.get<3>();
			
			DEBUG_OUT("%s ",name.c_str());
			DEBUG(range.dump());
			DEBUG_OUT("\n");
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			
			if(mer_node_r->find(name) != mer_node_r->end()){
				//mer_node_r->insert(std::pair<std::string, mercury_instance>(name, mercury_instance(range)));
				msgpack::type::tuple<int, std::string> ok_assign_range(OP::OK_ASSIGN_RANGE, name);
				tuple_send(ok_assign_range,org);
			}else{
				msgpack::type::tuple<int, std::string> ng_assign_range(OP::NG_ASSIGN_RANGE, name);
				tuple_send(ng_assign_range,org);
			}
			break;
		}
		case OP::OK_ASSIGN_RANGE:{
			DEBUG_OUT("OK_ASSIGN_RANGE:");
			break;
		}
		case OP::NG_ASSIGN_RANGE:{
			DEBUG_OUT("NG_ASSIGN_RANGE:");
			break;
		}
		case OP::DEL_RANGE:{
			DEBUG_OUT("DEL_RANGEE");
			
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
					
					//DEBUG(lock_mut lock(&mut););
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
			DEBUG_OUT("fd:%d ",fd());;
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
		pthread_mutex_init(&mut,NULL);
	
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
		
		address myaddress = address(settings.myip,settings.myport);
		
		msgpack::type::tuple<int,address> add_me_dy((int)OP::ADD_ME_DY, myaddress);
		
		{
			msgpack::vrefbuffer vbuf;
			msgpack::pack(vbuf,add_me_dy);
			const struct iovec* iov(vbuf.vector());
			writev(masterfd,iov, vbuf.vector_size());
		}
		msgpack::type::tuple<int,address> add_me_mer((int)OP::ADD_ME_MER, myaddress);
		{
			msgpack::vrefbuffer vbuf;
			msgpack::pack(vbuf,add_me_mer);
			const struct iovec* iov(vbuf.vector());
			writev(masterfd,iov, vbuf.vector_size());
		}
	}
	
	// mpio start
	lo.run(4);
} 
