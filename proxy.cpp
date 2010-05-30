#include <stdlib.h>
#include <mp/wavy.h>
#include <mp/sync.h>
#include <unordered_set>
#include <msgpack.hpp>
#include <msgpack/type/unordered_map.hpp>
#include "hash64.h"
#include "hash32.h"
#include "random64.h"
#include "tcp_wrap.h"
#include "address.hpp"
#include "sockets.hpp"
#include "merdy_operations.h"
#include <limits.h>
#include "debug_mode.h"
#include "mercury_objects.hpp"
#include "dynamo_objects.hpp"
#include "sqlparser.hpp"
#include <iostream>

#include <boost/program_options.hpp>
#include <unordered_map>

static const char interrupt[] = {-1,-12,-1,-3,6};

static struct settings{
	int verbose;
	unsigned short myport,masterport;
	int myip,masterip;
	int i_am_master;
	int mercury_cnt;
	pthread_cond_t hub_cond_waiting, hashes_cond_waiting;
	settings():verbose(10),myport(11011),masterport(11011),myip(get_myip()),masterip(aton("127.0.0.1")),i_am_master(1){}
}settings;

std::set<address> dynamo_nodes;
std::set<address> merdy_nodes;
mp::sync< std::map<uint64_t,address> > dy_hash;

address search_address(uint64_t key){
	mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
	assert(!dy_hash_r->empty());
	key &= ~((1<<8)-1);
	std::map<uint64_t,address>::const_iterator hash_it = dy_hash_r->upper_bound(key);
	if(hash_it == dy_hash_r->end()){
		hash_it = dy_hash_r->begin();
	}
	if(hash_it->second == address(1,0)){
		std::map<uint64_t,address>::const_iterator it = dy_hash_r->begin();
		while(it != dy_hash_r->end()){
			if(key < it->first) {
				++it;
				continue;
			}
			fprintf(stderr,"%lu->",it->first);
			it->second.dump();
			break;
		}
		assert(!"arien");
	}
	return hash_it->second;
}
socket_set sockets;
template<typename tuple>
inline void tuple_send(const tuple& t, const address& ad){
	msgpack::vrefbuffer vbuf;
	msgpack::pack(vbuf, t);
	const struct iovec* iov(vbuf.vector());
	sockets.writev(ad, iov, vbuf.vector_size());
}


mp::sync< std::unordered_multimap<uint64_t, address> > set_fwd; // set, fd
mp::sync< std::unordered_multimap<uint64_t, address> > coordinate_fwd; // coordinate
mp::sync< std::unordered_multimap<uint64_t, std::pair<int,address> > > put_fwd; // counter and origin address
mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > > send_fwd; // counter and value with origin address

mp::sync< std::unordered_multimap<std::string, mercury_instance> > mer_node;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> > mer_get_fwds;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_set_fwd*,mer_fwd_id_hash> > mer_set_fwds;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> > mer_range_fwd;

mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > > mer_hubs;

void dump_hashes(){
	mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
	
	std::map<uint64_t,address>::iterator it = dy_hash_r->begin();
	while(it != dy_hash_r->end()){
		it->second.dump();
		it++;
	}
}

class sql_insert_workingset{
public:
	std::list<std::string> attr_names;
	std::list<attr> attr_values;
	std::unordered_map<std::string,attr> tuple;
	uint64_t hashed_tuple;
	sql_insert_workingset(void)
		:attr_names(),attr_values(),tuple(),hashed_tuple(0){};
	
	std::string execute(){
		std::list<std::string>::iterator names_it = attr_names.begin();
		std::list<attr>::iterator values_it = attr_values.begin();
		
		mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
		while(names_it != attr_names.end()){
			std::unordered_map<std::string,std::map<attr_range,address> >::const_iterator hub_it = mer_hubs_r->find(*names_it);
			if(hub_it == mer_hubs_r->end()){
				return *names_it;
				break;
			}
			++names_it;
		}
		names_it = attr_names.begin();
		
		while(names_it != attr_names.end()){
			int mercury_query_identifier = __sync_fetch_and_add(&settings.mercury_cnt,1);
			std::list<mercury_kvp> valuelist;
			valuelist.push_back(mercury_kvp(*values_it, hashed_tuple));
			const MERDY::set_attr set_attr(OP::SET_ATTR, *names_it, mercury_query_identifier,valuelist,address(settings.myip,settings.myport));
			
			std::unordered_map<std::string,std::map<attr_range,address> >::iterator hub_map_it;
			hub_map_it = mer_hubs_r->find(*names_it); // it must find
			assert(hub_map_it != mer_hubs_r->end());
			
			std::map<attr_range,address>& target_hub = hub_map_it->second;
			std::map<attr_range,address>::iterator hub_it = target_hub.begin();
			while(hub_it != target_hub.end()){
				if(hub_it->first.contain(*values_it) || hub_it->first.is_invalid()){
					tuple_send(set_attr, hub_it->second);
					break;
				}
				++hub_it;
			}
			DEBUG_OUT("send..\n");
			++names_it; ++values_it;
		}
		
		// create tuple 
		names_it = attr_names.begin();
		values_it = attr_values.begin();
		assert(attr_names.size() == attr_values.size());
		std::unordered_map<std::string,attr> dynamo_tuple;
		while(names_it != attr_names.end()){
			dynamo_tuple.insert(std::pair<std::string,attr>(*names_it,*values_it));
			++names_it;++values_it;
		}
		const MERDY::set_dy set_dy(OP::SET_DY, hashed_tuple, dynamo_tuple,address(settings.myip,settings.myport));
				
		// send_tuple
		const address& dy_target = search_address(hashed_tuple);
		tuple_send(set_dy, dy_target);
		DEBUG_OUT("set dy for ");
		DEBUG(search_address(hashed_tuple).dump());
		DEBUG_OUT("\n");
		return std::string();
	}
};
mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> > > suspending_inserts;

namespace select_where{
class binary;
class unary;
class statement{
public:
	std::list<mercury_kvp> hashes;
	virtual bool execute() = 0;
};

class binary: public statement{
public:
	statement* lhs;
	int op;
	statement* rhs;
	binary(statement* _lhs, int _op, statement* _rhs)
		:lhs(_lhs),op(_op),rhs(_rhs){}
	bool execute(){
		// op is [and]/[or]
		if(!this->hashes.empty()){
			return true;
		}
		if(lhs->execute() == false || rhs->execute() == false){
			return false;
		}
		std::list<mercury_kvp>::iterator it = hashes.begin();
		if(op == sqlparser::sql::op_and){
			std::list<mercury_kvp>::iterator lhs_it = lhs->hashes.begin();
			std::list<mercury_kvp>::iterator common;
			while(lhs_it != lhs->hashes.end()){
				common = find(rhs->hashes.begin(), rhs->hashes.end(), *lhs_it);
				if(common != rhs->hashes.end()){
					this->hashes.push_back(*lhs_it);
				}
				++lhs_it;
			}
			delete lhs;
			delete rhs;
			this->hashes.sort();
		}else if(op == sqlparser::sql::op_or){
			this->hashes.merge(lhs->hashes);
			this->hashes.merge(rhs->hashes);
			delete lhs;
			delete rhs;
			this->hashes.sort();
			this->hashes.unique();
		}
	}
};
class unary: public statement{
public:
	int op;
	statement* target;
	std::list<uint64_t> hashes;
	bool execute(){};
};
class node: public statement{
public:
	const std::string name;
	const int op;
	const attr cond;
	node(const std::string& _name, const int& _op, const attr& _cond)
		:name(_name),op(_op),cond(_cond){};
	bool execute(){};
};

node* term(sqlparser::query* sql, std::list<statement*>* statements){
	if(sql->get() == sqlparser::sql::data){
		std::string name = sql->get().get_data().get_string();
		sql->next();
		int op = sql->get().get_query();
		sql->next();
		attr cond = sql->get().get_data();
		return new node(name, op, cond);
	}
}
statement* expression(sqlparser::query* sql, std::list<statement*>* statements){
	node* lhs = term(sql, statements);
	if(sql->next() == true){
		int op = sql->get().get_query();
		sql->next();
		node* rhs = term(sql, statements);
		return new binary(lhs,op,rhs);
	}else{
		return lhs;
	}
}
void where_parser(sqlparser::query* sql, std::list<statement*>* statements){
	statements->push_back(expression(sql,statements));
}
}// namespace select_where

class sql_select_workingset{
public:
	std::list<std::string> select_names;
	std::list<select_where::statement*> statements;
	
};
mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_select_workingset> > > suspending_selects;

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
	
	void event_handle(int, msgpack::object obj, msgpack::zone*){
		msgpack::type::tuple<int> out(obj);
		int operation = out.get<0>();
		switch (operation){
			/* ---------------------- */
			//   proxy operations
			/* ---------------------- */
		case OP::DO_SQL:{
			DEBUG_OUT("DO SQL:");
			const msgpack::type::tuple<int,std::string> do_sql(obj);
			const std::string sql_line = do_sql.get<1>();
			fprintf(stderr,"%s\n",sql_line.c_str());
			sqlparser::query sql(sql_line);
			const sqlparser::segment& seg = sql.get();
			if(seg == sqlparser::sql::create){
				sql.next();
				assert(sql.get() == sqlparser::sql::table); sql.next();
				assert(sql.get().is_data());
				//const std::string& tablename = sql.get().get_data().get_string();
				sql.next();
				assert(sql.get() == sqlparser::sql::left_brac); sql.next();
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_data());
					const std::string& attrname = sql.get().get_data().get_string(); sql.next();
					int type = attr::invalid;
					
					if(sql.get() == sqlparser::sql::data_char){
						type = attr::string;
					}else if(sql.get() == sqlparser::sql::data_int){
						type = attr::number;
					}else{
						assert(!"invalid data");
					}
					sql.next();
					
					const MERDY::create_schema create_schema(OP::CREATE_SCHEMA,attrname,type,address(settings.myip,settings.myport));
					tuple_send(create_schema,address(settings.masterip,settings.masterport));
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}
					DEBUG_OUT("query create_schema sent.");
				}
			}else if(seg == sqlparser::sql::insert){
				//DEBUG_OUT("INSERT INTO:");
				sql.next();
				std::shared_ptr<sql_insert_workingset> sets_p(new sql_insert_workingset());
				assert(sql.get() == sqlparser::sql::into); sql.next();
				assert(sql.get().is_data());
				//const std::string& tablename = sql.get().get_data().get_string();
				sql.next();
				assert(sql.get() == sqlparser::sql::left_brac); sql.next();
				
				// get names
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_data()); 
					sets_p->attr_names.push_back(sql.get().get_data().get_string());
					sql.next();
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}
				}
				sql.next();
				assert(sql.get() == sqlparser::sql::values);sql.next();
				assert(sql.get() == sqlparser::sql::left_brac);sql.next();
				// get attrs
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_data());
					sets_p->attr_values.push_back(sql.get().get_data());
					sets_p->hashed_tuple ^= sql.get().get_data().hash64();
					/*
					  DEBUG_OUT("parse-data :");
					  DEBUG(sql.get().get_data().dump());
					  DEBUG_OUT("\n");
					//*/
					sql.next();
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}else{
						assert(sql.get() == sqlparser::sql::right_brac || sql.get().is_data());
					}
				}
				
				// set attrs to mercury
				const std::string& lack_hub = sets_p->execute();
				if(lack_hub == std::string()){
					DEBUG_OUT("success.");
				}else{
					{
						mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> > >::ref suspending_inserts_r(suspending_inserts);
						suspending_inserts_r->insert(std::pair<std::string,std::shared_ptr<sql_insert_workingset> >(lack_hub, sets_p));
						DEBUG_OUT("suspend it for hub %s\n",lack_hub.c_str());
					}
					
					const MERDY::tellme_assign tellme_assign(OP::TELLME_ASSIGN, lack_hub, address(settings.myip,settings.myport));
					tuple_send(tellme_assign, address(settings.masterip,settings.masterport));
				}
			}else if(seg == sqlparser::sql::select){
				// -------------------------------working space--------------------------------
				DEBUG_OUT("SELECT:");
				sql.next();
				std::shared_ptr<sql_select_workingset> work_p(new sql_select_workingset());
				while(1){
					if(sql.get().is_data()){
						work_p->select_names.push_back(sql.get().get_data().get_string());
						sql.next();
					}else if(sql.get() == sqlparser::sql::op_star){
						work_p->select_names.clear();
						sql.next();
						break;
					}else if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}else{
						break;
					}
				}
				if(sql.get() == sqlparser::sql::from){
					sql.next();
					assert(sql.get().is_data());
					//sql.get().get_data().get_string(); // FIXME:table name is disabled now 
					sql.next();
				}
				//std::list<> where_exps;
				if(sql.get() == sqlparser::sql::where){
					sql.next();
					select_where::where_parser(&sql,&work_p->statements);
				}
				// -------------------------------working space--------------------------------
			}else{
				DEBUG_OUT("invalid message\n");
				assert(!"arienai");
			}
			break;
		}
		case OP::OK_CREATE_SCHEMA:{
			DEBUG_OUT("OK_CREATE_SCHEMA:");
			const MERDY::ok_create_schema ok_create_schema(obj);
			const std::string& name = ok_create_schema.get<1>();
			const std::map<attr_range,address>& newhub = ok_create_schema.get<2>();
			
			DEBUG_OUT("%s\n",name.c_str());
			mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
			mer_hubs_r->insert(std::pair<std::string, std::map<attr_range,address> >(name,newhub));
			break;
		}
		case OP::ASSIGNMENT:{
			DEBUG_OUT("ASSIGNMENT:");
			const MERDY::assignment assignment(obj);
			const std::string& name = assignment.get<1>();
			const std::map<attr_range,address>& hub_nodes = assignment.get<2>();
			{
				mp::sync< std::unordered_map<std::string, std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
				std::unordered_map<std::string, std::map<attr_range,address> >::iterator hub_it = mer_hubs_r->find(name);
				if(hub_it == mer_hubs_r->end()){
					std::map<attr_range,address> newhub;
					std::map<attr_range,address>::const_iterator nodes = hub_nodes.begin();
					mer_hubs_r->insert(std::pair<std::string, std::map<attr_range,address> >(name, hub_nodes));
				}
			}
			mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> > >::ref suspending_inserts_r(suspending_inserts);
			while(suspending_inserts_r->size() > 0){
				std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> >::iterator sus_it = suspending_inserts_r->find(name);
				if(sus_it == suspending_inserts_r->end()){break;}
				const std::string& lack_hub = sus_it->second->execute();
				if(lack_hub == std::string()){
					suspending_inserts_r->erase(sus_it);
					DEBUG_OUT("done\n");
				}else{
					suspending_inserts_r->insert(std::pair<std::string,std::shared_ptr<sql_insert_workingset> >(lack_hub,sus_it->second));
					suspending_inserts_r->erase(sus_it);
					
					const MERDY::tellme_assign tellme_assign(OP::TELLME_ASSIGN, lack_hub, address(settings.myip,settings.myport));
					tuple_send(tellme_assign, address(settings.masterip,settings.masterport));
					DEBUG_OUT("suspended for %s\n",lack_hub.c_str());
				}
			}
			break;
		}
			/* ---------------------- */
			// Dynamo operations
			/* ---------------------- */
		case OP::UPDATE_HASHES:{
			DEBUG_OUT("UPDATE_HASHES:");
			const msgpack::type::tuple<int,std::map<uint64_t,address> > out(obj);
			const std::map<uint64_t,address>& tmp_dy_hash = out.get<1>();
			std::map<uint64_t,address>::const_iterator it = tmp_dy_hash.begin();
			mp::sync< std::map<uint64_t, address> >::ref dy_hash_r(dy_hash);
			while(it != tmp_dy_hash.end()){
				dy_hash_r->insert(*it);
				++it;
			}
			pthread_cond_broadcast(&settings.hashes_cond_waiting);
			DEBUG_OUT("%d hash received\n",(int)tmp_dy_hash.size());
			break;
		}
		case OP::OK_SET_DY:{// op, key, address
			DEBUG_OUT("OK_SET_DY:");
			// responce for fowarding
			MERDY::ok_set_dy ok_set_dy(obj);
			const uint64_t& key = ok_set_dy.get<1>();
			
			DEBUG_OUT("key:%lu ok.\n",key);
			break;
		}
		case OP::FOUND_DY:{ // op, key, vcvalue, address
			DEBUG_OUT("FOUND_DY:");
			const MERDY::found_dy found_dy(obj);
			const uint64_t& key = found_dy.get<1>();
			const value_vclock& value = found_dy.get<2>();
			const address& org = found_dy.get<3>();
			
			mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > >::ref send_fwd_r(send_fwd);
			std::unordered_multimap<uint64_t, get_fwd_t>::iterator it = send_fwd_r->find(key);
			if(it == send_fwd_r->end()){
				DEBUG_OUT("%lu already answered\n", key);
				
				for(std::unordered_multimap<uint64_t, get_fwd_t>::iterator it=send_fwd_r->begin();it != send_fwd_r->end(); ++it){
					fprintf(stderr,"key[%lu],",it->first);
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
			MERDY::notfound_dy notfound_dy(obj);
			const uint64_t& key = notfound_dy.get<1>();
			const address& org = notfound_dy.get<2>();
			DEBUG_OUT("for %lu\n",key);
			
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
			// Mercury_Instancery operations
			/* ---------------------- */
		case OP::UPDATE_MER_HUB:{// op, std::map<address>
			DEBUG_OUT("UPDATE_MER_HUB:");
			msgpack::type::tuple<int, std::set<address> > update_mer_hub(obj);
			std::set<address>& tmp_mer_hub = update_mer_hub.get<1>();
			std::set<address>::iterator it = tmp_mer_hub.begin();
			break;
		}
		case OP::OK_SET_ATTR:{ // op, attr_name
			DEBUG_OUT("OK_SET_ATTR:");
			
			break;
		}
		case OP::OK_GET_RANGE:{ 
			DEBUG_OUT("OK_GET_RANGE:");
			const MERDY::ok_get_range ok_get_range(obj);
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
				const MERDY::ok_get_range ok_get_range(OP::OK_GET_RANGE, name, identifier, it->second->toSend);
				tuple_send(ok_get_range, it->second->org);
				delete it->second;
				mer_range_fwd_r->erase(it);
				DEBUG_OUT("ok fowarding.\n");
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
			fprintf(stderr,"invalid message:");
			std::cout << "object received: " << obj << std::endl;
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
					
					//DEBUG(std::cout << "object received: " << msg << "->");
					
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
	settings.mercury_cnt=0;
	settings.hub_cond_waiting = PTHREAD_COND_INITIALIZER;
	settings.hashes_cond_waiting = PTHREAD_COND_INITIALIZER;
	
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
		const MERDY::tellme_hashes tellme_hashes(OP::TELLME_HASHES,address(settings.myip,settings.myport));
		tuple_send(tellme_hashes,address(settings.masterip,settings.masterport));
	}
	
	// mpio start
	lo.run(4);
} 
