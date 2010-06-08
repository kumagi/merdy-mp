#include <stdlib.h>
#include <mp/wavy.h>
#include <mp/sync.h>
#include <unordered_map>
#include <unordered_set>
#include <msgpack.hpp>
#include "unordered_map.hpp"
#include "hash64.h"
#include "hash32.h"
#include "random64.h"
#include "tcp_wrap.h"
#include "address.hpp"
#include "sockets.hpp"
#include <limits.h>
#include "debug_mode.h"
#include "merdy_operations.h"
#include "mercury_objects.hpp"
#include "dynamo_objects.hpp"




#include <boost/program_options.hpp>
#include <unordered_map>

static const char interrupt[] = {-1,-12,-1,-3,6};

static struct settings{
	int verbose;
	std::string interface;
	unsigned short myport,masterport;
	int myip,masterip;
	int i_am_master;
	settings():verbose(10),myport(11011),masterport(11011),myip(get_myip()),masterip(aton("127.0.0.1")),i_am_master(1){}
}settings;

std::set<address> dynamo_nodes;
std::set<address> merdy_nodes;
std::map<uint64_t,address> dy_hash;
socket_set sockets;

// dynamo
mp::sync< std::unordered_multimap<uint64_t, address> > set_fwd; // set, fd
mp::sync< std::unordered_multimap<uint64_t, address> > coordinate_fwd; // coordinate flag
mp::sync< std::unordered_multimap<uint64_t, std::pair<int,address> > > put_fwd; // counter and origin address
mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > > send_fwd; // counter and value with origin address
mp::sync< std::unordered_multimap<uint64_t, value_vclock> > key_value;

// mercury
mp::sync< std::unordered_multimap<std::string, mercury_instance> > mer_node;

mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> > mer_get_fwds;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_set_fwd,mer_fwd_id_hash> > mer_set_fwds;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> > mer_range_fwd;


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


template<typename tuple>
inline void tuple_send_async(const tuple* t, const address& ad, mp::shared_ptr<msgpack::zone> z){
	msgpack::vrefbuffer vbuf;
	msgpack::pack(vbuf, *t);
	const struct iovec* iov(vbuf.vector());
	sockets.writev(ad, iov, vbuf.vector_size(),z);
}


class main_handler : public mp::wavy::handler {
	mp::wavy::loop* lo;
	msgpack::unpacker m_pac;
	
public:
	main_handler(int _fd,mp::wavy::loop* _lo):mp::wavy::handler(_fd),lo(_lo){
	}
	
	void event_handle(int , msgpack::object obj, mp::shared_ptr<msgpack::zone> z){
		msgpack::type::tuple<int> op(obj);
		int operation = op.get<0>();
		switch (operation){
			/* ---------------------- */
			// Dynamo operations
			/* ---------------------- */
		case OP::UPDATE_HASHES:{
			DEBUG_OUT("UPDATE_HASHES:");
			const MERDY::update_hashes& update_hashes(obj);
			const std::map<uint64_t,address>& tmp_dy_hash = update_hashes.get<1>();
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
			const MERDY::set_dy& set_dy(obj);
			const uint64_t& key = set_dy.get<1>();
			const std::unordered_map<std::string,attr>& value = set_dy.get<2>();
			const address& org = set_dy.get<3>();
			
			DEBUG(std::cout << "key[" << key << "] value[");
			for(std::unordered_map<std::string,attr>::const_iterator it = value.begin(); it!=value.end(); ++it){
				DEBUG_OUT(" [%s->",it->first.c_str());
				DEBUG(it->second.dump());
				DEBUG_OUT("]");
			}
			DEBUG(std::cout << "]" << std::endl);
			
			uint64_t hash = hash64(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			if(it == dy_hash.end()){
				it = dy_hash.begin();
			}
			const address& coordinator = it->second;
			DEBUG(it->second.dump());
			{
				mp::sync< std::unordered_multimap<uint64_t, address> >::ref set_fwd_r(set_fwd);
				set_fwd_r->insert(std::pair<uint64_t,address>(key,org));
			}
			
			MERDY::set_coordinate set_coordinate((int)OP::SET_COORDINATE, key, value, address(settings.myip,settings.myport));
			tuple_send(set_coordinate,coordinator);
			
			DEBUG_OUT("\n");
			break;
		}
		case OP::OK_SET_DY:{// op, key, address
			DEBUG_OUT("OK_SET_DY:");
			// responce for fowarding
			const MERDY::ok_set_dy& ok_set_dy(obj);
			const uint64_t& key = ok_set_dy.get<1>();
			
			mp::sync< std::unordered_multimap<uint64_t, address> >::ref set_fwd_r(set_fwd);
			std::unordered_multimap<uint64_t,address>::iterator it = set_fwd_r->find(key);
			if(it == set_fwd_r->end()){
				DEBUG_OUT("not found\n");
				break;
			}
			{
				MERDY::ok_set_dy ok_set_dy(OP::OK_SET_DY,key);
				tuple_send(ok_set_dy,it->second);
				set_fwd_r->erase(it);
			}
			break;
		}
		case OP::SET_COORDINATE:{// op, key, value, address
			DEBUG_OUT("SET_COORDINATE:");
			const MERDY::set_coordinate& set_coordinate(obj);
			const uint64_t& key = set_coordinate.get<1>();
			const std::unordered_map<std::string,attr>& value = set_coordinate.get<2>();
			const address& org = set_coordinate.get<3>();
			mp::sync< std::unordered_multimap<uint64_t, std::pair<int,address> > >::ref put_fwd_r(put_fwd);
			put_fwd_r->insert(std::pair<uint64_t, std::pair<int, address> >(key, std::pair<int, address>(0,org)));
			
			uint64_t hash = hash64(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			if(it == dy_hash.end()){
				it = dy_hash.begin(); // hashtable is ring
			}
			
			value_vclock vcvalue;
			{
				mp::sync< std::unordered_multimap<uint64_t, value_vclock> >::ref key_value_r(key_value);
				std::unordered_multimap<uint64_t, value_vclock>::iterator result = key_value_r->find(key);
				if(result == key_value_r->end()){
					vcvalue.update(value);
				}else{
					vcvalue.update(value,result->second.get_clock());
				}
				key_value_r->insert(std::pair<uint64_t, value_vclock>(key,vcvalue));
			}
			++it;
			if(it == dy_hash.end()){
				it = dy_hash.begin();
			}
			
			int tablesize = dy_hash.size();
			std::unordered_set<address, address_hash> already_send;
			for(int i=DY::NUM; i>0; --i){
				const address& target = it->second;
				if(already_send.find(target) == already_send.end()){
					MERDY::put_dy put_dy((int)OP::PUT_DY, key, vcvalue, address(settings.myip,settings.myport));
					DEBUG_OUT("%llu ->",(unsigned long long)key);
					DEBUG(vcvalue.dump());
					tuple_send(put_dy, target);
					DEBUG(target.dump());
					DEBUG_OUT("\n");
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
			const MERDY::put_dy& put_dy(obj);
			const uint64_t& key = put_dy.get<1>();
			const value_vclock& value = put_dy.get<2>();
			const address& ad = put_dy.get<3>();
			
			{
				mp::sync< std::unordered_multimap<uint64_t, value_vclock> >::ref key_value_r(key_value);
				std::unordered_multimap<uint64_t, value_vclock>::iterator it = key_value_r->find(key);
				if(it == key_value_r->end()){
					// new insert
					key_value_r->insert(std::pair<uint64_t, value_vclock>(key, value_vclock(value)));
					DEBUG_OUT("saved:%llu-> ",(unsigned long long)key);
					DEBUG(value.dump(););
					DEBUG_OUT(" new!\n");
				}else{
					it->second.update(value.get_value());
					DEBUG_OUT("saved:%llu->",(unsigned long long)key);
					DEBUG(value.dump(););
					DEBUG_OUT("\n");
				}
			}
			
			MERDY::ok_put_dy ok_put_dy(OP::OK_PUT_DY, key);
			tuple_send(ok_put_dy,ad);
			break;
		}
		case OP::OK_PUT_DY:{// op, key
			DEBUG_OUT("OK_PUT_DY:");
			// ack for PUT_DY, only coordinator should receives it
			const MERDY::ok_put_dy& out(obj);
			const uint64_t& key = out.get<1>();
			mp::sync< std::unordered_multimap<uint64_t, std::pair<int,address> > >::ref put_fwd_r(put_fwd);
			std::unordered_multimap<uint64_t, std::pair<int, address> >::iterator it = put_fwd_r->find(key);
			if(it == put_fwd_r->end()){
				break;
			}
			++(it->second.first);
			if(it->second.first == DY::WRITE){
				// write ok
				DEBUG_OUT("write ok:[%llu]\n",(unsigned long long)key);
				MERDY::ok_set_dy ok_set_dy(OP::OK_SET_DY, key);
				tuple_send(ok_set_dy,it->second.second);
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
			const MERDY::get_dy& get_dy(obj);
			const uint64_t& key = get_dy.get<1>();
			const address& org = get_dy.get<2>();
			
			uint64_t hash = hash64(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			if(it == dy_hash.end()){
				it = dy_hash.begin();
			}
			{
				mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > >::ref send_fwd_r(send_fwd);
				send_fwd_r->insert(std::pair<uint64_t, get_fwd_t>(key, get_fwd_t(value_vclock(),org)));
			}
			int tablerest = dy_hash.size();
			std::unordered_set<address, address_hash> sentlist;
			for(int i=DY::NUM; i>0; --i){
				if(sentlist.find(it->second) == sentlist.end()){
					MERDY::send_dy send_dy((int)OP::SEND_DY, key, address(settings.myip,settings.myport));
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
			
			DEBUG_OUT("key:%llu\n",(unsigned long long)key);
			break;
		}
		case OP::GET_MULTI_DY:{
			DEBUG_OUT("GET_MULTI_DY:");
			const MERDY::get_multi_dy& get_multi_dy(obj);
			const std::list<uint64_t>& key_list = get_multi_dy.get<1>();
			const address& org = get_multi_dy.get<2>();
			break;
		}
		case OP::SEND_DY:{
			DEBUG_OUT("SEND_DY:");
			const MERDY::send_dy& send_dy(obj);
			const uint64_t& key = send_dy.get<1>();
			const address& org = send_dy.get<2>();
			
			mp::sync< std::unordered_multimap<uint64_t, value_vclock> >::ref key_value_r(key_value);
			std::unordered_multimap<uint64_t, value_vclock>::const_iterator ans
				= key_value_r->find(key);
			if(ans == key_value_r->end()){
				const MERDY::notfound_dy notfound_dy((int)OP::NOTFOUND_DY, key, address(settings.myip,settings.myport));
				tuple_send(notfound_dy, org);
			}else{
				const MERDY::found_dy found_dy((int)OP::FOUND_DY, key, ans->second, address(settings.myip,settings.myport));
				tuple_send(found_dy, org);
				DEBUG_OUT("found ");
				DEBUG(ans->second.dump());
				DEBUG_OUT("\n");
			}
			break;
		}
		case OP::FOUND_DY:{ // op, key, vcvalue, address
			DEBUG_OUT("FOUND_DY:");
			const MERDY::found_dy& found_dy(obj);
			const uint64_t& key = found_dy.get<1>();
			const value_vclock& value = found_dy.get<2>();
			const address& org = found_dy.get<3>();
			
			mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > >::ref send_fwd_r(send_fwd);
			std::unordered_multimap<uint64_t, get_fwd_t>::iterator it = send_fwd_r->find(key);
			if(it == send_fwd_r->end()){
				DEBUG_OUT("%llu already answered\n", (unsigned long long)key);
				
				for(std::unordered_multimap<uint64_t, get_fwd_t>::iterator it=send_fwd_r->begin();it != send_fwd_r->end(); ++it){
					DEBUG_OUT("key[%llu],",(unsigned long long)it->first);
				}
				break;
			}
			int result = it->second.update(value);
			if(result == -1){ // old data found -> read repair
				const MERDY::put_dy put_dy(OP::PUT_DY, key, it->second.get_vcvalue(), address(settings.myip,settings.myport));
				tuple_send(put_dy, org);
			}
			if(it->second.count_eq(DY::READ)){
				DEBUG_OUT("counter:%d ",it->second.get_cnt());
				mp::sync< std::unordered_multimap<uint64_t, value_vclock> >::ref key_value_r(key_value);
				std::unordered_multimap<uint64_t, value_vclock>::iterator ans = key_value_r->find(key);
				
				if(ans != key_value_r->end()){
					ans->second.update(it->second.get_value());
				}else{
					key_value_r->insert(std::pair<uint64_t, value_vclock>(it->first,it->second.get_value()));
				}
				const MERDY::found_dy found_dy(OP::FOUND_DY,key,it->second.get_vcvalue(),address(settings.myip,settings.myport));
				tuple_send(found_dy, it->second.org);
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
			const MERDY::notfound_dy& notfound_dy(obj);
			const uint64_t& key = notfound_dy.get<1>();
			const address& org = notfound_dy.get<2>();
			DEBUG_OUT("for %llu\n",(unsigned long long)key);
			
			mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > >::ref send_fwd_r(send_fwd);
			std::unordered_multimap<uint64_t, get_fwd_t>::iterator it = send_fwd_r->find(key);
			
			if(it != send_fwd_r->end()){
				// read repair
				const MERDY::put_dy put_dy((int)OP::PUT_DY, key, it->second.get_vcvalue(), address(settings.myip,settings.myport));
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
			const MERDY::update_mer_hub& update_mer_hub(obj);
			const std::set<address>& tmp_mer_hub = update_mer_hub.get<1>();
			std::set<address>::iterator it = tmp_mer_hub.begin();
			
			break;
		}
		case OP::ASSIGN_ATTR:{ // op, mercury_instance
			break;
		}
		case OP::SET_ATTR:{ // op, attr_name, list<mercury_kvp>, address
			DEBUG_OUT("SET_ATTR:");
			const MERDY::set_attr& set_attr(obj);
			const std::string& name = set_attr.get<1>();
			const int& identifier = set_attr.get<2>();
			const mercury_kvp& kvp = set_attr.get<3>(); 
			const address& org = set_attr.get<4>();
			
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it_mi = mer_node_r->find(name);
			std::unordered_multimap<std::string, mercury_instance>::iterator it_mi_bak = it_mi;
			
			if(it_mi== mer_node_r->end()){
				DEBUG_OUT("i dont have attr %s\n",name.c_str());
			}
			
			DEBUG_OUT("set kvp in [%s] ",name.c_str());
			
			int ok=0;
			while(it_mi != mer_node_r->end() && it_mi->first == name){
				if(it_mi->second.get_range().contain(kvp.attr_)){
					it_mi->second.kvp.insert(std::pair<attr,uint64_t>(kvp.get_attr(),kvp.get_hash()));
					ok = 1;
					
					DEBUG(kvp.get_attr().dump());
					DEBUG_OUT("%llu inserted in  and %lu entries in",(unsigned long long)kvp.get_hash(), it_mi->second.kvp.size());
					DEBUG(it_mi->second.get_range().dump());
					DEBUG_OUT("\n");
					
					const MERDY::ok_set_attr ok_set_attr(OP::OK_SET_ATTR, name, identifier);
					tuple_send(ok_set_attr, org);
					break;
				}
				++it_mi;
			}
			if(!ok){// pass SET_ATTR to correct node
				DEBUG_OUT(" passed\n");
				it_mi = it_mi_bak;
				
				mp::sync< std::unordered_multimap<mer_fwd_id, mer_set_fwd, mer_fwd_id_hash> >::ref mer_set_fwds_r(mer_set_fwds);
				mer_set_fwds_r->insert(std::pair<mer_fwd_id,mer_set_fwd>(mer_fwd_id(name,identifier),mer_set_fwd(1,org)));
				
				std::map<attr_range, address>::const_iterator target = it_mi->second.get_hubs().begin();
				while(target != it_mi->second.get_hubs().end()){
					if(target->first.contain(kvp.get_attr())){
						break;
					}
					++target;
				}
				
				assert(target->second != address(settings.myip,settings.myport) && target != it_mi->second.get_hubs().end());
				
				const MERDY::set_attr pass_set_attr(OP::SET_ATTR,name,identifier,kvp,address(settings.myip,settings.myport));
				tuple_send(pass_set_attr,target->second);
			}
			DEBUG_OUT("done\n");
			break;
		}
		case OP::OK_SET_ATTR:{ // op, attr_name
			DEBUG_OUT("OK_SET_ATTR:");
			const MERDY::ok_set_attr& ok_set_attr(obj);
			const std::string& name = ok_set_attr.get<1>();
			const int& identifier = ok_set_attr.get<2>();
			
			mp::sync< std::unordered_multimap<mer_fwd_id, mer_set_fwd, mer_fwd_id_hash> >::ref mer_set_fwds_r(mer_set_fwds);
			std::unordered_multimap<mer_fwd_id,mer_set_fwd>::iterator it = mer_set_fwds_r->find(mer_fwd_id(name,identifier));
			
			assert(it != mer_set_fwds_r->end());
			it->second.cnt--;
			if(it->second.cnt == 0){
				MERDY::ok_set_attr ok_set_attr(OP::OK_SET_ATTR, name, identifier);
				tuple_send(ok_set_attr,it->second.org);
				mer_set_fwds_r->erase(it);
			}
			break;
		}
		case OP::GET_RANGE:{ // op, attr_name, range, org_addres
			DEBUG_OUT("GET_RANGE:");
			const MERDY::get_range& get_range(obj);
			const std::string& name = get_range.get<1>();
			const int& identifier = get_range.get<2>();
			const attr_range& range = get_range.get<3>();
			const address& org = get_range.get<4>();
			DEBUG_OUT(" for %s. range is",name.c_str());
			DEBUG(range.dump());
			
			//mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it_mi = mer_node.unsafe_ref().find(name);
			
			// dup
			std::multimap<attr,uint64_t>& key_value = it_mi->second.kvp;
			const attr_range& myrange = it_mi->second.get_range();
			const std::map<attr_range, address>& hub = it_mi->second.get_hubs();
			
			std::list<mercury_kvp> answer;
			if(myrange.contain(range)){
				std::multimap<attr, uint64_t>::iterator it = key_value.lower_bound(range.get_begin());
				if(range.get_begin().is_invalid()){
					it = key_value.begin();
				}
				DEBUG_OUT(" ");
				while(it != key_value.end() && (range.contain(it->first) || range.get_end().is_invalid())){
					answer.push_back(mercury_kvp(it->first,it->second));
					DEBUG(it->first.dump());
					++it;
				}
			}
			
			std::list<std::pair<address,attr_range> > fwd;
			mer_get_fwd* newfwd = NULL;
			if((range.get_begin().is_invalid() && !myrange.get_begin().is_invalid()) ||
			   (range.get_end().is_invalid() && !myrange.get_end().is_invalid()) ||
			   range.get_begin() < myrange.get_begin() || myrange.get_end() < range.get_end()){ // if out of my range
				int counter = 0;
				std::map<attr_range, address>::const_iterator hub_it = hub.begin();
				
				// left limit
				if(hub.begin()->second == address(settings.myip,settings.myport)){
				}else if((--hub.end())->second == address(settings.myip,settings.myport)){
				}else{
					while(hub_it != hub.end()){
						if(range.contain(hub_it->first) && hub_it->second != address(settings.myip,settings.myport)){
							attr_range common;
							range.get_common_range(hub_it->first, &common);
						
							fwd.push_back(std::pair<address,attr_range>(hub_it->second,common));
							counter++;
							DEBUG(hub_it->first.dump());
							DEBUG_OUT("range foward!\n");
							break;
						}
						++hub_it;
					}
					assert(hub_it != hub.end());
					mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> >::ref mer_range_fwd_r(mer_range_fwd);
					newfwd = new mer_get_fwd(answer,counter+1,org);
					mer_range_fwd_r->insert(std::pair<mer_fwd_id, mer_get_fwd*>(mer_fwd_id(name,identifier),newfwd));
				}
			}
			
			if(!fwd.empty()){
				std::list<std::pair<address,attr_range> >::iterator it = fwd.begin();
				while(it != fwd.end()){
					const MERDY::get_range* get_range
						= z->allocate<MERDY::get_range>(OP::GET_RANGE,name,identifier,it->second,address(settings.myip,settings.myport)); 
					tuple_send_async(get_range,it->first,z);
					++it;
				}
				newfwd->cnt--;
				if(newfwd->cnt == 0){
					mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> >::ref mer_range_fwd_r(mer_range_fwd);
					const MERDY::ok_get_range* const ok_get_range
						= z->allocate<MERDY::ok_get_range>(OP::OK_GET_RANGE, name, identifier, newfwd->toSend);
					tuple_send_async(ok_get_range, newfwd->org,z);
					mer_range_fwd_r->erase(mer_fwd_id(name,identifier));
					delete newfwd;
				}
				DEBUG_OUT("fowarded\n");
			}else{
				answer.sort();
				const MERDY::ok_get_range* const ok_get_range 
					= z->allocate<MERDY::ok_get_range>(OP::OK_GET_RANGE, name, identifier, answer);
				tuple_send_async(ok_get_range, org,z);
				DEBUG(org.dump());
				DEBUG_OUT("not fowarded\n");
			}
			break;
		}
		case OP::OK_GET_RANGE:{ 
			DEBUG_OUT("OK_GET_RANGE:");
			const MERDY::ok_get_range& ok_get_range(obj);
			const std::string& name = ok_get_range.get<1>();
			const int& identifier = ok_get_range.get<2>();
			const std::list<mercury_kvp>& ans = ok_get_range.get<3>();
			std::list<mercury_kvp> copy_ans = ans;
			mp::sync< std::unordered_multimap<mer_fwd_id,
					mer_get_fwd*, mer_fwd_id_hash> >::ref mer_range_fwd_r(mer_range_fwd);
			
			std::unordered_multimap<mer_fwd_id,mer_get_fwd*>::iterator it = mer_range_fwd_r->find(mer_fwd_id(name,identifier));
			it->second->toSend.merge(copy_ans);
			it->second->cnt--;
			if(it->second->cnt == 0){
				it->second->toSend.sort();
				const MERDY::ok_get_range* const ok_get_range = z->allocate<MERDY::ok_get_range>(OP::OK_GET_RANGE, name, identifier, it->second->toSend);
				tuple_send_async(ok_get_range, it->second->org,z);
				delete it->second;
				mer_range_fwd_r->erase(it);
				DEBUG_OUT("ok fowarding.\n");
			}
			break;
		}
		case OP::GET_ATTR:{
			DEBUG_OUT("GET_ATTR:");
			const MERDY::get_attr& get_attr(obj);
			const std::string& name = get_attr.get<1>();
			const int& identifier = get_attr.get<2>();
			const std::list<attr>& requires = get_attr.get<3>();
			const address& org = get_attr.get<4>();
			
			DEBUG_OUT("searching for %s\n", name.c_str());
			
			//mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			
			std::unordered_multimap<std::string, mercury_instance>::const_iterator it = mer_node.unsafe_ref().find(name);
			std::unordered_multimap<std::string, mercury_instance>::const_iterator it_bak = it;
			
			std::list<mercury_kvp> ans;
			
			std::list<attr>::const_iterator req = requires.begin();
			
			mp::sync< std::unordered_multimap<mer_fwd_id, mer_get_fwd*, mer_fwd_id_hash> >::ref mer_get_fwds_r(mer_get_fwds);
			mer_get_fwd* fowarding = NULL;
			
			
			std::list<std::pair<address,std::list<attr> > > foward_list;
			while(req != requires.end()){
				int ok=0;
				it = it_bak;
				while(it != mer_node.unsafe_ref().end() && it->first == name){
					if(it->second.get_range().contain(*req)){
						ok = 1;
						const std::multimap<attr, uint64_t>& kvp = it->second.kvp;
						std::multimap<attr, uint64_t>::const_iterator found_kvp = kvp.find(*req);
						while(found_kvp != kvp.end() && found_kvp->first == *req){
							ans.push_back(mercury_kvp(found_kvp->first,found_kvp->second));
							DEBUG(found_kvp->first.dump());
							DEBUG_OUT(" -> %llu ok\n", (unsigned long long)found_kvp->second);
							++found_kvp;
						}
						DEBUG(req->dump());
						DEBUG_OUT("not found. for %lu entries\n", kvp.size());
					}
					++it;
				}
				if(!ok){ // forwarding other instance
					it = it_bak;
					if(fowarding == NULL){ //  if its first time
						fowarding = new mer_get_fwd(1, org);
						mer_get_fwds_r->insert(std::pair<mer_fwd_id,mer_get_fwd*>
											   (mer_fwd_id(name, identifier),
												fowarding));
					}
					const std::map<attr_range, address>& hub = it->second.get_hubs();
					std::map<attr_range, address>::const_iterator target = hub.begin();
					while(target != hub.end()){
						if(target->first.contain(*req)){
							break;
						}
						++target;
					}
					
					assert(target != hub.end());
					
					std::list<std::pair<address,std::list<attr> > >::iterator fwd_it = foward_list.begin(); 
					while(fwd_it != foward_list.end()){
						if(fwd_it->first == target->second){
							break;
						}
						++fwd_it;
					}
					if(fwd_it == foward_list.end()){
						fowarding->cnt++;
						std::list<attr> newlist;
						fwd_it = foward_list.insert(fwd_it,std::pair<address,std::list<attr> >(target->second,newlist));
					}
					fwd_it->second.push_back(*req);
					DEBUG_OUT("forwarding !!\n");
				}
				++req;
			}
			if(fowarding){
				fowarding->toSend.merge(ans); 
				std::list<std::pair<address,std::list<attr> > >::iterator fwd_it = foward_list.begin(); 
				while(fwd_it != foward_list.end()){
					const MERDY::get_attr get_attr(OP::GET_ATTR, name, identifier, fwd_it->second, address(settings.myip,settings.myport));
					tuple_send(get_attr, fwd_it->first);
					++fwd_it;
				}
				fowarding->cnt--;
			}else if(!ans.empty()){
				const MERDY::ok_get_attr ok_get_attr(OP::OK_GET_ATTR, name, identifier, ans);
				tuple_send(ok_get_attr, org);
				DEBUG(org.dump());
				DEBUG_OUT("sending OK_GET_ATTR \n");
			}else{
				const MERDY::ng_get_attr ng_get_attr(OP::NG_GET_ATTR, name, identifier);
				tuple_send(ng_get_attr, org);
			}
			break;
		}
		case OP::OK_GET_ATTR:{
			DEBUG_OUT("OK_GET_ATTR:");
			const MERDY::ok_get_attr& ok_get_attr(obj);
			const std::string& name = ok_get_attr.get<1>();
			const int& identifier = ok_get_attr.get<2>();
			std::list<mercury_kvp> ans = ok_get_attr.get<3>();
			
			/*
			  std::list<mercury_kvp>::iterator list_it = ans.begin();
			  while(list_it != ans.end()){
			  list_it->dump();
			  ++list_it;
			  }
			//*/
			
			// it must have been fowarded
			mp::sync< std::unordered_multimap<mer_fwd_id, mer_get_fwd*, mer_fwd_id_hash> >::ref mer_get_fwds_r(mer_get_fwds);
			std::unordered_multimap<mer_fwd_id, mer_get_fwd*>::iterator it = mer_get_fwds_r->find(mer_fwd_id(name,identifier));
			assert(it != mer_get_fwds_r->end());
			
			it->second->cnt--;
			it->second->toSend.merge(ans);
			if(it->second->cnt == 0){
				it->second->toSend.sort();
				MERDY::ok_get_attr ok_get_attr(OP::OK_GET_ATTR,name,identifier,it->second->toSend);
				tuple_send(ok_get_attr, it->second->org);
				DEBUG(it->second->org.dump());
				delete it->second;
				mer_get_fwds_r->erase(it);
				DEBUG_OUT("sending OK_GET_ATTR\n");
			}
			break;
		}
		case OP::NG_GET_ATTR:{
			DEBUG_OUT("NG_GET_ATTR:");
			const MERDY::ng_get_attr& ng_get_attr(obj);
			const std::string& name = ng_get_attr.get<1>();
			const int& identifier = ng_get_attr.get<2>();
			fprintf(stderr," for %s\n",name.c_str());
			
			mp::sync< std::unordered_multimap<mer_fwd_id, mer_get_fwd*, mer_fwd_id_hash> >::ref mer_get_fwds_r(mer_get_fwds);
			std::unordered_multimap<mer_fwd_id, mer_get_fwd*>::iterator it = mer_get_fwds_r->find(mer_fwd_id(name,identifier));
			assert(it != mer_get_fwds_r->end());
			
			it->second->cnt--;
			if(it->second->cnt == 0){
				MERDY::ok_get_attr ok_get_attr(OP::OK_GET_ATTR,name,identifier,it->second->toSend);
				tuple_send(ok_get_attr, it->second->org);
				delete it->second;
				mer_get_fwds_r->erase(it);
			}
			break;
		}
		case OP::TELLME_RANGE:{ // op, attrname, address
			DEBUG_OUT("TELLME_RANGE:");
 			const MERDY::tellme_range& tellme_range(obj);
			const std::string& name = tellme_range.get<1>();
			const address& org = tellme_range.get<2>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			if(it != mer_node_r->end()){
				MERDY::ok_tellme_range ok_tellme_range(OP::OK_TELLME_RANGE, name, it->second.get_range());
				tuple_send(ok_tellme_range, org);
			}else{
				MERDY::ng_tellme_range ng_tellme_range(OP::NG_TELLME_RANGE, name);
				tuple_send(ng_tellme_range, org);
			}
			break;
		}
		case OP::OK_TELLME_RANGE:{
			DEBUG_OUT("OK_TELLME_RANGE:");
			const MERDY::ok_tellme_range& ok_tellme_range(obj);
			const std::string& name = ok_tellme_range.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			
			break;
		}
		case OP::NG_TELLME_RANGE:{
			DEBUG_OUT("NG_TELLME_RANGE:");
			const MERDY::ng_tellme_range& ng_tellme_range(obj);
			const std::string& name = ng_tellme_range.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			break;
		}
		case OP::GIVEME_RANGE:{
			DEBUG_OUT("GIVEME_RANGE:");
			break;
		}
		case OP::ASSIGN_RANGE:{// op, name, range, hubs
			DEBUG_OUT("ASSIGN_RANGE:");
			const MERDY::assign_range& assign_range(obj);
			const std::string& name = assign_range.get<1>();
			const attr_range& range = assign_range.get<2>();
			const std::map<attr_range, address>& hub = assign_range.get<3>();
			const address& org = assign_range.get<4>();
			
			DEBUG_OUT("%s ",name.c_str());
			DEBUG(range.dump());
			DEBUG_OUT("\n");
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			
			//if(mer_node_r->find(name) != mer_node_r->end()){
			mer_node_r->insert(std::pair<std::string, mercury_instance>(name, mercury_instance(range,hub)));
			MERDY::ok_assign_range ok_assign_range(OP::OK_ASSIGN_RANGE, name);
			tuple_send(ok_assign_range,org);
			
			DEBUG_OUT("assign_range done\n");
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
		default:{
			DEBUG_OUT("unrecognized\n");
		}
		}
	}
	void on_read(mp::wavy::event& e)
	{
		try{
			while(true) {
				if(m_pac.execute()) {
					msgpack::object msg = m_pac.data();
					mp::shared_ptr<msgpack::zone> z( m_pac.release_zone() );
					m_pac.reset();

					e.more();  //e.next();
					
					//DEBUG(std::cout << "object received: " << msg << "->");
					
					//DEBUG(lock_mut lock(&mut););
					event_handle(fd(), msg, z);
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
		}
		catch(msgpack::type_error& e) {
			DEBUG_OUT("on_read: type error");
			throw;
		} catch(std::exception& e) {
			DEBUG_OUT("on_read: ");
			throw;
		} catch(...) {
			DEBUG_OUT("on_read: unknown error");
			e.remove();
			throw;
		}
	}
};

void on_accepted(int fd, int err, mp::wavy::loop* lo)
{
	DEBUG_OUT("accept %d %d\n",fd,err);
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
		("interface,i",po::value<std::string>(&settings.interface)->default_value("eth0"), "my interface")
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
	settings.myip = get_myip_interface2(settings.interface.c_str());
	
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
