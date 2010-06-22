#include <stdlib.h>
#include <mp/wavy.h>
#include <mp/sync.h>
#include <unordered_set>
#include <msgpack.hpp>
#include "unordered_map.hpp"
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
#include "proxy_mercury_objects.cpp"

#include <boost/program_options.hpp>
#include <unordered_map>

static const char interrupt[] = {-1,-12,-1,-3,6};

#include "sql_insertion_objects.cpp"

static struct settings{
	int verbose;
	std::string interface;
	unsigned short myport,masterport;
	int myip,masterip;
	int i_am_master;
	int mercury_cnt;
	int dynamo_cnt;
	pthread_cond_t hub_cond_waiting, hashes_cond_waiting;
	settings():verbose(10),myport(11011),masterport(11011),myip(get_myip()),masterip(aton("127.0.0.1")),i_am_master(1){}
}settings;

std::set<address> namo_nodes;
std::set<address> merdy_nodes;
mp::sync< std::map<uint64_t,address> > dy_hash;

address search_address(uint64_t key){
	mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
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
			DEBUG_OUT("%llu->",(unsigned long long)it->first);
			DEBUG(it->second.dump());
			break;
		}
		assert(!"arien");
	}
	return hash_it->second;
}
socket_set sockets;


mp::sync< std::unordered_multimap<uint64_t, address> > set_fwd; // set, fd
mp::sync< std::unordered_multimap<uint64_t, address> > coordinate_fwd; // coordinate
mp::sync< std::unordered_multimap<uint64_t, std::pair<int,address> > > put_fwd; // counter and origin address
mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > > send_fwd; // counter and value with origin address

mp::sync< std::unordered_multimap<std::string, mercury_instance> > mer_node;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> > mer_get_fwds;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_set_fwd*,mer_fwd_id_hash> > mer_set_fwds;
mp::sync< std::unordered_multimap<mer_fwd_id,mer_get_fwd*,mer_fwd_id_hash> > mer_range_fwd;

mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > > mer_hubs;

mp::sync< std::unordered_multimap<std::string, std::pair<attr, MERDY::get_attr> > > suspended_select_get_attr;
mp::sync< std::unordered_multimap<std::string, std::pair<attr, MERDY::get_range> > > suspended_select_get_range;

mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> > > suspended_insert_mercury;

template<typename tuple>
bool mercury_send(const std::string& name, const attr& target, const tuple& t){
	mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
	std::unordered_map<std::string,std::map<attr_range,address> >::iterator hub_it = mer_hubs_r->find(name);
	if(hub_it != mer_hubs_r->end()){
		std::map<attr_range,address>::iterator range_it = hub_it->second.begin();
		while(range_it != hub_it->second.end()){
			if(range_it->first.contain(target)){
				tuple_send(t,range_it->second, &sockets);
				return true;
			}
			++range_it;
		}
		if(range_it == hub_it->second.end()){
			DEBUG_OUT("cannot send\n");
		}
		return false;
	}else{
		DEBUG_OUT("no hub to search\n");
		return false;
	}
}
void dump_hashes(){
	mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
	
	std::map<uint64_t,address>::iterator it = dy_hash_r->begin();
	while(it != dy_hash_r->end()){
		it->second.dump();
		it++;
	}
}

class unary{
public:
	const std::string name;
	const enum sqlparser::sql::sql operation;
	const attr condition;
	
	bool resolved_flag;
	std::list<mercury_kvp> hashes;
	unary(const std::string& _name, enum sqlparser::sql::sql& _operation,const attr& _condition)
		:name(_name),operation(_operation),condition(_condition){}
	unary()
		:name(),operation(sqlparser::sql::invalid),condition(){}
	
	void dump()const{
		fprintf(stderr,"[%s", name.c_str());
		switch ((int)operation){
		case sqlparser::sql::op_eq:{
			fprintf(stderr,"=");
			break;
		}
		case sqlparser::sql::op_gt:{
			fprintf(stderr,">");
			break;
		}
		case sqlparser::sql::op_lt:{
			fprintf(stderr,"<");
			break;
		}
		default:{
			assert(!"arienai");
			break;
		}
		}
		condition.dump();
		fprintf(stderr,"]");
	}
};

class statement{
public:
	enum type{
		factor,
		operation,
	};
	const enum type factor_or_operation;
	unary factor_;
	const enum sqlparser::sql::sql operation_;
	
	statement(const unary& _factor)
		:factor_or_operation(factor),factor_(_factor),operation_(sqlparser::sql::invalid){}
	statement(const enum sqlparser::sql::sql& _operation)
		:factor_or_operation(operation),factor_(),operation_(_operation){}
	statement(const statement& org)
		:factor_or_operation(org.factor_or_operation),factor_(org.factor_),operation_(org.operation_){}
	bool is_factor()const{
		return factor_or_operation == factor;
	}
	bool is_operation()const{
		return factor_or_operation == operation;
	}
	void dump()const{
		if(factor_or_operation == factor){
			factor_.dump();
		}else if(factor_or_operation == operation){
			switch ((int)operation_){
			case sqlparser::sql::op_and:{
				fprintf(stderr,"&");
				break;
			}
			case sqlparser::sql::op_or:{
				fprintf(stderr,"|");
				break;
			}
			default:{
				assert(!"arienai");
				break;
			}
			}
		}
	}
private:
	statement();
};

class sql_select_dynamo{
public:
	const std::list<std::string> select_names;
	std::list<value_vclock> found_data;
	const int org_fd;
	mp::wavy::loop* lo_;
	std::string prefix;
	sql_select_dynamo(const std::list<std::string> _select_names,const int& _org_fd, mp::wavy::loop* const _lo,const std::string& _prefix)
		:select_names(_select_names),org_fd(_org_fd),lo_(_lo),prefix(_prefix){}
	~sql_select_dynamo(){
		DEBUG_OUT("-------answer-----\n");
		std::list<value_vclock>::iterator data_it = found_data.begin();
		std::string answer = prefix;
		while(data_it != found_data.end()){
			const std::unordered_map<std::string,attr>& data = data_it->get_value();
			std::list<std::string>::const_iterator names_it = select_names.begin();
			while(names_it != select_names.end()){
				const std::unordered_map<std::string,attr>::const_iterator ans_node = data.find(*names_it);
				if(ans_node == data.end()){
					DEBUG_OUT("%s not found",names_it->c_str());
					break;}
				if(ans_node->second.is_int()){
					char tmpbuff[256];
					sprintf(tmpbuff,"%d",ans_node->second.get_int());
					answer += std::string(tmpbuff);
				}else if(ans_node->second.is_string()){
					answer += ans_node->second.get_string();
				}
				++names_it;
				if(names_it != select_names.end()){
					answer += std::string("\t");
				}
			}
			++data_it;
			
			if(data_it != found_data.end()){
				answer += std::string("\n");
			}
		}
		DEBUG_OUT("%s\n------------------\n", answer.c_str());
		lo_->write(org_fd,answer.data(),answer.length());
	}
private:
	sql_select_dynamo();
};


mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_select_dynamo> > > sql_suspended_select_dynamo;
class sql_select_workingset{
public:
	std::list<std::string> select_names;
	std::unordered_map<mercury_request_fowards,statement*,mercury_request_fowards_hash> statements;
	std::list<statement> expression;
	std::list<mercury_request_fowards> waiting_queries;
	const int org_fd;
	bool count_flag;
	mp::wavy::loop* lo_;
	sql_select_workingset(const int& _org_fd, const bool _count_flag, mp::wavy::loop* _lo):org_fd(_org_fd),count_flag(_count_flag),lo_(_lo){}
	
	void dump_statement()const {
		std::list<statement>::const_iterator st_it = expression.begin();
		while(st_it != expression.end()){
			st_it->dump();
			++st_it;
		}
	}	
	~sql_select_workingset(){
		// get_attr and get_range is end
		DEBUG_OUT("start to dynamo.\n");
		
		std::list<statement>::iterator exp_it = expression.begin();
		std::list<std::list<mercury_kvp> > workingset;
		
		// statement dump
		DEBUG(dump_statement());
		
		while(exp_it != expression.end()){
			if(exp_it->is_factor()){
				workingset.push_back(exp_it->factor_.hashes);
			}else if(exp_it->is_operation()){
				std::list<mercury_kvp> lhs = workingset.back();
				workingset.pop_back();
				std::list<mercury_kvp> rhs = workingset.back();
				workingset.pop_back();
				
				std::list<mercury_kvp> newlist;
				if(exp_it->operation_ == sqlparser::sql::op_and){
					DEBUG(int lhs_size = lhs.size());
					DEBUG(int rhs_size = rhs.size());
					
					std::list<mercury_kvp>::iterator lhs_it = lhs.begin();
					while(lhs_it != lhs.end()){
						std::list<mercury_kvp>::iterator rhs_it = rhs.begin();
						while(rhs_it != rhs.end()){
							if(kvp_hash_equal()(*rhs_it,*lhs_it)){
								/*
								  if(!(rhs_it->get_attr() == lhs_it->get_attr())){
								  rhs_it->dump();
								  fprintf(stderr," == ");
								  lhs_it->dump();
								  fprintf(stderr,"\n");
								  }
								*/
								newlist.push_back(*lhs_it);
								break;
							}
							++rhs_it;
						}
						++lhs_it;
					}
					newlist.sort();
					newlist.unique(kvp_hash_equal());
					
					//fprintf(stderr,"%d & %d -> %d\n",lhs_size, rhs_size, newlist.size());
				}else if(exp_it->operation_ == sqlparser::sql::op_or){
					newlist.merge(lhs);
					newlist.merge(rhs);
					newlist.sort();
					newlist.unique(kvp_hash_equal());
				}else{
					assert(!"invalid operation");
				}
				workingset.push_back(std::list<mercury_kvp>());
				workingset.back().merge(newlist);
			}
			++exp_it;
		}
		assert(workingset.size() == 1);
		std::list<mercury_kvp>& answer_kvps = workingset.front();
		std::list<mercury_kvp>::iterator ans_it = answer_kvps.begin();
		
		/*
		  while(ans_it != answer_kvps.end()){
		  //DEBUG_OUT("%llu ",ans_it->get_hash());
		  DEBUG(ans_it->dump());
		  DEBUG_OUT("\n");
		  ++ans_it;
		  }
		  DEBUG_OUT(" size (%lu)\n",answer_kvps.size());
		*/
		
		std::list<mercury_kvp>& target_tuples = workingset.front();
		std::list<mercury_kvp>::iterator tuple_it = target_tuples.begin();
		
		std::string prefix;
		
		char buff[256];
		sprintf(buff,"%lu tuples\n",answer_kvps.size());
		prefix.assign(buff);
		
		std::shared_ptr<sql_select_dynamo> dynamo_work(new sql_select_dynamo(select_names,org_fd,lo_,prefix));
		
		if(!count_flag){
			mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_select_dynamo> > >::ref sql_suspended_select_dynamo_r(sql_suspended_select_dynamo);
			while(tuple_it != target_tuples.end()){
				sql_suspended_select_dynamo_r->insert(std::pair<uint64_t, std::shared_ptr<sql_select_dynamo> >(tuple_it->get_hash(),dynamo_work));
				mp::shared_ptr<msgpack::zone> z(new msgpack::zone());
				const MERDY::get_dy* get_dy = z->allocate<MERDY::get_dy>(OP::GET_DY,tuple_it->get_hash(),address(settings.myip,settings.myport));
				const address& target = search_address(tuple_it->get_hash()); 
				tuple_send_async(get_dy,target,&sockets,z);
				++tuple_it;
			}
		}
	
	}
	void listup_id(){
		std::list<statement>::const_iterator exp_it = expression.begin();
		//fprintf(stderr,"pushing(%lu)\n",expression.size());
		while(exp_it != expression.end()){
			DEBUG(exp_it->dump());
			if(exp_it->is_factor()){
				const std::string& name = exp_it->factor_.name;
				const int identifier = __sync_fetch_and_add(&settings.mercury_cnt,1);
				waiting_queries.push_back(mercury_request_fowards(name,identifier));
				//fprintf(stderr,"pushed %s:%d \n",name.c_str(),identifier);
			}
			++exp_it;
			DEBUG_OUT(",");
		}
		DEBUG_OUT("push done");
	}
	void do_get_attr(){
		assert(!waiting_queries.empty());
		std::list<statement>::iterator exp_it = expression.begin();
		std::list<mercury_request_fowards>::const_iterator req_it = waiting_queries.begin();
		while(exp_it != expression.end()){
			if(exp_it->is_factor()){
				const enum sqlparser::sql::sql& operation = exp_it->factor_.operation;
				const attr& condition = exp_it->factor_.condition;
				statements.insert(std::pair<mercury_request_fowards,statement*>(*req_it,&*exp_it));
				
				if(operation == sqlparser::sql::op_eq){
					std::list<attr> tmp_attrs;
					tmp_attrs.push_back(condition);
					MERDY::get_attr get_attr(OP::GET_ATTR,req_it->name,req_it->identifier,tmp_attrs,address(settings.myip,settings.myport));
					if(!mercury_send(req_it->name, condition,get_attr)){
						mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_attr> > >::ref suspended_select_get_attr_r(suspended_select_get_attr);
						suspended_select_get_attr_r->insert(std::pair<std::string, std::pair<attr,MERDY::get_attr> >(req_it->name, std::pair<attr,MERDY::get_attr>(condition, get_attr)));	// 
					}
				}else if(operation == sqlparser::sql::op_gt){
					MERDY::get_range get_range(OP::GET_RANGE,req_it->name, req_it->identifier, attr_range(condition,attr()), address(settings.myip,settings.myport));
					if(!mercury_send(req_it->name, condition,get_range)){
						mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> > >::ref suspended_select_get_range_r(suspended_select_get_range);
						suspended_select_get_range_r->insert(std::pair<std::string, std::pair<attr,MERDY::get_range> >(req_it->name, std::pair<attr,MERDY::get_range>(condition, get_range)));
					}
				}else if(operation == sqlparser::sql::op_lt){
					MERDY::get_range get_range(OP::GET_RANGE,req_it->name, req_it->identifier, attr_range(attr(),condition), address(settings.myip,settings.myport));
					if(!mercury_send(req_it->name, condition,get_range)){
						mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> > >::ref suspended_select_get_range_r(suspended_select_get_range);
						suspended_select_get_range_r->insert(std::pair<std::string, std::pair<attr,MERDY::get_range> >(req_it->name, std::pair<attr,MERDY::get_range>(condition, get_range)));
					}
				}
				++req_it;
			}
			++exp_it;
		}
	}
	const std::list<mercury_request_fowards>& get_waiting_queries()const{
		assert(!waiting_queries.empty());
		return waiting_queries;
	}
	void dump()const{
		std::list<statement>::const_iterator it = expression.begin();
		while(it != expression.end()){
			it->dump();
			++it;
		}
	}
private:
	sql_select_workingset();
};
void parse_expression(sqlparser::query* sql, sql_select_workingset* sets);

void parse_factor(sqlparser::query* sql, sql_select_workingset* sets){
	if(sql->get() == sqlparser::sql::invalid){
		assert(sql->get().is_string());
		std::string name(sql->get().get_string());
		sql->next();
		
		assert(sql->get().is_query());
		enum sqlparser::sql::sql operation = sql->get().get_query();
		sql->next();
		
		attr cond;
		if(sql->get().is_number()){
			cond.set(sql->get().get_number());
		}else{
			cond.set_string(sql->get().get_string());
		}
		
		sets->expression.push_back(statement(unary(name,operation,cond)));
		sql->next();
	}else if(sql->get() == sqlparser::sql::left_brac){
		sql->next();
		parse_expression(sql,sets);
		assert(sql->get() == sqlparser::sql::right_brac);
		sql->next();
	}else{
		assert(false && "invalid expression");
	}
}

void parse_term(sqlparser::query* sql, sql_select_workingset* sets){
	parse_factor(sql,sets);
	while(sql->get() == sqlparser::sql::op_and){
		sql->next();
		//assert(sql->get().is_string());
		parse_factor(sql, sets);
		sets->expression.push_back(statement(sqlparser::sql::op_and));
	}
}
void parse_expression(sqlparser::query* sql, sql_select_workingset* sets){
	parse_term(sql, sets);
	while(sql->get() == sqlparser::sql::op_or){
		sql->next();
		//assert(sql->get().is_string());
		parse_term(sql, sets);
		sets->expression.push_back(statement(sqlparser::sql::op_or));
	}
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


mp::sync< std::unordered_map<mercury_request_fowards, std::shared_ptr<sql_select_workingset>, mercury_request_fowards_hash> > suspending_selects;
mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > > sql_create_table;
mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> > > sql_insert_into_dynamo;

class main_handler : public mp::wavy::handler {
	mp::wavy::loop* lo;
	msgpack::unpacker m_pac;
public:
	main_handler(int _fd,mp::wavy::loop* _lo):mp::wavy::handler(_fd),lo(_lo){
	}
	
	void event_handle(int fd, msgpack::object obj, mp::shared_ptr<msgpack::zone> z){
		msgpack::type::tuple<int> out(obj);
		int operation = out.get<0>();
		switch (operation){
			/* ---------------------- */
			//   proxy operations
			/* ---------------------- */
		case OP::DO_SQL:{
			DEBUG_OUT("DO SQL:");
			const msgpack::type::tuple<int,std::string>& do_sql(obj);
			const std::string& sql_line = do_sql.get<1>();
			//fprintf(stderr,"%s\n",sql_line.c_str());
			sqlparser::query sql(sql_line);
			const sqlparser::segment& seg = sql.get();
			if(seg == sqlparser::sql::create){
				sql.next();
				assert(sql.get() == sqlparser::sql::table); sql.next();
				assert(sql.get().is_string());
				//const std::string& tablename = sql.get().get_data().get_string();
				sql.next();
				assert(sql.get() == sqlparser::sql::left_brac); sql.next();
								
				std::shared_ptr<sql_answer> ans(new sql_answer(fd,std::string("done.\n")));
				while(!(sql.get() == sqlparser::sql::right_brac)){
					
					assert(sql.get().is_string());
					const std::string& attrname = sql.get().get_string(); sql.next();
					int type;
					if(sql.get() == sqlparser::sql::data_char){
						type = attr::string;
					}else if(sql.get() == sqlparser::sql::data_int){
						type = attr::number;
					}else{
                        type = 0;
						assert(!"invalid data");
					}
					sql.next();
					{
						mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > >::ref sql_create_table_r(sql_create_table);
						sql_create_table_r->insert(std::pair<std::string,std::shared_ptr<sql_answer> >(attrname, ans));
					}
					const MERDY::create_schema* const  create_schema = z->allocate<MERDY::create_schema>((int)OP::CREATE_SCHEMA,attrname,type,address(settings.myip,settings.myport));
					tuple_send_async(create_schema,address(settings.masterip,settings.masterport),&sockets,z);
					DEBUG_OUT("%s create.\n",attrname.c_str());
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}
				}
				DEBUG_OUT("done.");
			}else if(seg == sqlparser::sql::insert){
				sql.next();
				std::list<std::string> attr_names;
				
				assert(sql.get() == sqlparser::sql::into); sql.next();
				assert(sql.get().is_string());
				//const std::string& tablename = sql.get().get_data().get_string();
				sql.next();
				assert(sql.get() == sqlparser::sql::left_brac); sql.next();
				
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_string()); 
					attr_names.push_back(sql.get().get_string());
					sql.next();
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}
				}
				sql.next();
				assert(sql.get() == sqlparser::sql::values);sql.next();
				assert(sql.get() == sqlparser::sql::left_brac);sql.next();
				// get attrs
				std::list<attr> attr_values;
				uint64_t hashed_tuple = 0;
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_string());
					if(sql.get().is_number()){
						attr_values.push_back(attr(sql.get().get_number()));
						hashed_tuple += attr(sql.get().get_number()).hash64();
					}else{
						attr_values.push_back(attr(sql.get().get_string()));
						hashed_tuple += attr(sql.get().get_string()).hash64();
					}
					sql.next();
					
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}else{
						assert(sql.get() == sqlparser::sql::right_brac || sql.get().is_string());
					}
				}
				sql.next(); // parsing end
				
				if(attr_names.size() != attr_values.size()){
					std::string answer("error: number of value and attributes not matched\n");
					lo->write(fd, answer.data(),answer.length());
					break;
				}
				
				{
					mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> > >::ref sql_insert_into_dynamo_r(sql_insert_into_dynamo);
					sql_insert_into_dynamo_r->insert(std::pair<uint64_t,std::shared_ptr<sql_answer> >(hashed_tuple, new sql_answer(fd, std::string("insertion complete\n"))));
				}
				
				// create and save tuple into dynamo
				std::list<std::string>::iterator names_it = attr_names.begin();
				std::list<attr>::iterator values_it = attr_values.begin();
				std::unordered_map<std::string,attr> dynamo_tuple;
				while(names_it != attr_names.end()){
					dynamo_tuple.insert(std::pair<std::string,attr>(*names_it,*values_it));
					++names_it;++values_it;
				}
				const MERDY::set_dy* set_dy = z->allocate<MERDY::set_dy>(OP::SET_DY, hashed_tuple, dynamo_tuple,address(settings.myip,settings.myport));
				const address& dy_target = search_address(hashed_tuple);
				tuple_send_async(set_dy, dy_target, &sockets, z);
				DEBUG_OUT("set dy for ");
				DEBUG(search_address(hashed_tuple).dump());
				DEBUG_OUT("\n");
					
				// set attrs to mercury
				mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
				while(names_it != attr_names.end()){
					std::unordered_map<std::string,std::map<attr_range,address> >::const_iterator hub_it = mer_hubs_r->find(*names_it);
					if(hub_it == mer_hubs_r->end()){
						break;
					}
					++names_it;
				}
				names_it = attr_names.begin();
				values_it = attr_values.begin();
				
				std::shared_ptr<sql_insert_workingset> sets_p(new sql_insert_workingset(hashed_tuple, fd, lo));
				while(names_it != attr_names.end()){
					std::unordered_map<std::string,std::map<attr_range,address> >::iterator hub_map_it;
					hub_map_it = mer_hubs_r->find(*names_it);
					if(hub_map_it != mer_hubs_r->end()){ // target hub find.
						const std::map<attr_range,address>& target_hub = hub_map_it->second;
						std::map<attr_range,address>::const_iterator hub_it = target_hub.begin();
						while(hub_it != target_hub.end()){
							if(hub_it->first.contain(*values_it)){
								const int mercury_query_identifier = __sync_fetch_and_add(&settings.mercury_cnt,1);
								const MERDY::set_attr* set_attr = z->allocate<MERDY::set_attr>(OP::SET_ATTR, *names_it, mercury_query_identifier ,mercury_kvp(*values_it,hashed_tuple), address(settings.myip,settings.myport));
								tuple_send_async(set_attr, hub_it->second, &sockets, z);
								break;
							}
							++hub_it;
						}
						assert(hub_it != target_hub.end());
					}else{
						{// locking suspended_insert_mercury
							mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> > >::ref suspended_insert_mercury_r(suspended_insert_mercury);
							sets_p->tuple.insert(std::pair<std::string,attr>(*names_it,*values_it));
							suspended_insert_mercury_r->insert(std::pair<std::string,std::shared_ptr<sql_insert_workingset> >(*names_it,sets_p));
						}
						
						const MERDY::tellme_assign* tellme_assign = z->allocate<MERDY::tellme_assign>(OP::TELLME_ASSIGN, *names_it, address(settings.myip,settings.myport));
						tuple_send_async(tellme_assign, address(settings.masterip,settings.masterport),&sockets, z);
					}
					++names_it; ++values_it;
				}
					
			}else if(seg == sqlparser::sql::select){
				DEBUG_OUT("select:");
				sql.next();
				std::shared_ptr<sql_select_workingset> work(new sql_select_workingset(fd,false,lo));
				while(1){
					if(sql.get().is_string()){
						work->select_names.push_back(sql.get().get_string());
						sql.next();
					}else if(sql.get() == sqlparser::sql::op_star){
						work->select_names.clear();
						sql.next();
						break;
					}else if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}else if(sql.get() == sqlparser::sql::count){
						work->count_flag = true;
						sql.next();
						break;
					}else{
						break;
					}
				}
				
				assert(sql.get() == sqlparser::sql::from);
				if(sql.get() == sqlparser::sql::from){
					sql.next();
					assert(sql.get().is_string());
					//sql.get().get_data().get_string(); // FIXME:table name is disabled now 
					sql.next();
				}
				
				assert(sql.get() == sqlparser::sql::where);
				sql.next();
				parse_expression(&sql,&*work);
				
				work->listup_id();
				
				const std::list<mercury_request_fowards>& requires_attrs = work->get_waiting_queries();
				std::list<mercury_request_fowards>::const_iterator req_it = requires_attrs.begin();
				mp::sync< std::unordered_map<mercury_request_fowards, std::shared_ptr<sql_select_workingset>, mercury_request_fowards_hash> >::ref
					suspending_selects_r(suspending_selects);
				while(req_it != requires_attrs.end()){
					suspending_selects_r->insert(std::pair<mercury_request_fowards, std::shared_ptr<sql_select_workingset> >(*req_it,work));
					++req_it;
				}
				work->do_get_attr();
				/*
				  mp::sync< std::multimap<mercury_request_fowards, std::shared_ptr<sql_select_workingset> > >::ref suspending_selects_r(suspending_selects);
				  std::list<mercury_request_fowards> unsolved = work_p->statements.front()->get_unsolved();
				  std::list<mercury_request_fowards>::iterator unsolved_it = unsolved.begin();
				  while(unsolved_it != unsolved.end()){
				  suspending_selects_r->insert(std::pair<mercury_request_fowards, std::shared_ptr<sql_select_workingset> >(*unsolved_it, work_p));
				  DEBUG_OUT("send for ");
				  DEBUG(unsolved_it->dump());
				  DEBUG_OUT("\n");
				  ++unsolved_it;
				  }
				  DEBUG(work_p->dump());
				//*/
			}else{
				DEBUG_OUT("invalid message\n");
				assert(!"arienai");
			}
			break;
		}
		case OP::OK_CREATE_SCHEMA:{
			DEBUG_OUT("OK_CREATE_SCHEMA:");
			const MERDY::ok_create_schema& ok_create_schema(obj);
			const std::string& name = ok_create_schema.get<1>();
			const std::map<attr_range,address>& newhub = ok_create_schema.get<2>();
			
			DEBUG_OUT("%s\n",name.c_str());
			mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
			mer_hubs_r->insert(std::pair<std::string, std::map<attr_range,address> >(name,newhub));
			
			{ // answer to client
				mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > >::ref sql_create_table_r(sql_create_table);
				sql_create_table_r->erase(name);
			}
			break;
		}
		case OP::NG_CREATE_SCHEMA:{
			DEBUG_OUT("NG_CREATE_SCHEMA:");
			const MERDY::ng_create_schema& ng_create_schema(obj);
			const std::string& name = ng_create_schema.get<1>();
			const std::map<attr_range,address>& newhub = ng_create_schema.get<2>();
			
			DEBUG_OUT("%s\n",name.c_str());
			mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
			mer_hubs_r->insert(std::pair<std::string, std::map<attr_range,address> >(name,newhub));
			
			{ // answer to client
				mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > >::ref sql_create_table_r(sql_create_table);
				sql_create_table_r->erase(name);
			}
			break;
		}
		case OP::ASSIGNMENT:{
			DEBUG_OUT("ASSIGNMENT:");
			const MERDY::assignment& assignment(obj);
			const std::string& name = assignment.get<1>();
			const std::map<attr_range,address>& hub_nodes = assignment.get<2>();
			DEBUG_OUT(" for %s\n",name.c_str());
			{
				mp::sync< std::unordered_map<std::string, std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
				std::unordered_map<std::string, std::map<attr_range,address> >::iterator hub_it = mer_hubs_r->find(name);
				if(hub_it == mer_hubs_r->end()){
					std::map<attr_range,address> newhub;
					std::map<attr_range,address>::const_iterator nodes = hub_nodes.begin();
					mer_hubs_r->insert(std::pair<std::string, std::map<attr_range,address> >(name, hub_nodes));
				}
			}
			
			mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> > >::ref suspended_insert_mercury_r(suspended_insert_mercury);
			std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> >::iterator sus_it;
			while((sus_it = suspended_insert_mercury_r->find(name)) != suspended_insert_mercury_r->end()){
				int mercury_query_identifier = __sync_fetch_and_add(&settings.mercury_cnt,1);
				
				const std::unordered_map<std::string,attr>& target_tuple = sus_it->second->tuple;
				const std::unordered_map<std::string,attr>::const_iterator tuple_it = target_tuple.find(name);
				assert(tuple_it != target_tuple.end());
				const attr& target_attr = tuple_it->second;
				const uint64_t& target_hash = sus_it->second->hashed_tuple;
				const MERDY::set_attr set_attr(OP::SET_ATTR,name,mercury_query_identifier, mercury_kvp(target_attr,target_hash),address(settings.myip,settings.myport));
				suspended_insert_mercury_r->erase(sus_it);
			}
			
			mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> > >::ref suspended_select_get_range_r(suspended_select_get_range);
			if(!suspended_select_get_range_r->empty()){
				DEBUG_OUT("thehre exists suspended select query\n");
				while(1){
					mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> > >::ref suspended_select_get_range_r(suspended_select_get_range);
					std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> >::const_iterator get_range_it = suspended_select_get_range_r->find(name);
					
					if(get_range_it == suspended_select_get_range_r->end()){
						DEBUG_OUT("not found about %s\n", name.c_str());
						break;
					}
					if(mercury_send(name, get_range_it->second.first, get_range_it->second.second)){
						DEBUG_OUT("executed for %s\n",name.c_str());
						suspended_select_get_range_r->erase(get_range_it);
					}
				}
			}
			mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_attr> > >::ref suspended_select_get_attr_r(suspended_select_get_attr);
			if(!suspended_select_get_attr_r->empty()){
				while(1){
					std::unordered_multimap<std::string, std::pair<attr,MERDY::get_attr> >::iterator get_attr_it = suspended_select_get_attr_r->find(name);
					if(get_attr_it == suspended_select_get_attr_r->end()){
						break;
					}
					if(mercury_send(name, get_attr_it->second.first, get_attr_it->second.second)){
						suspended_select_get_attr_r->erase(get_attr_it);
					}
				}
			}
			
			break;
		}
			/* ---------------------- */
			// Dynamo operations
			/* ---------------------- */
		case OP::UPDATE_HASHES:{
			DEBUG_OUT("UPDATE_HASHES:");
			const msgpack::type::tuple<int,std::map<uint64_t,address> >& out(obj);
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
			const MERDY::ok_set_dy& ok_set_dy(obj);
			const uint64_t& key = ok_set_dy.get<1>();
			
			mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> > >::ref 
				sql_insert_into_dynamo_r(sql_insert_into_dynamo);
			std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> >::iterator it = sql_insert_into_dynamo_r->find(key);
			sql_insert_into_dynamo_r->erase(it);
			
			DEBUG_OUT("key:%llu ok.\n",(unsigned long long)key);
			break;
		}
		case OP::FOUND_DY:{ // op, key, vcvalue, address
			DEBUG_OUT("FOUND_DY:");
			const MERDY::found_dy& found_dy(obj);
			const uint64_t& key = found_dy.get<1>();
			const value_vclock& value = found_dy.get<2>();
			//const address& org = found_dy.get<3>();
			DEBUG(value.dump());
			DEBUG_OUT("\n");
			{
				mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_select_dynamo> > >::ref
					sql_suspended_select_dynamo_r(sql_suspended_select_dynamo);
				std::unordered_multimap<uint64_t, std::shared_ptr<sql_select_dynamo> >::iterator it
					= sql_suspended_select_dynamo_r->find(key);
				if(it == sql_suspended_select_dynamo_r->end()){
					DEBUG_OUT("size[%lu]  ",sql_suspended_select_dynamo_r->size());
					DEBUG_OUT("not found"); assert(false);break;}
				it->second->found_data.push_back(value);
				DEBUG_OUT(" rest:%lu\n",sql_suspended_select_dynamo_r->size()-1);
				sql_suspended_select_dynamo_r->erase(it);
			}
			break;
		}
		case OP::NOTFOUND_DY:{
			DEBUG_OUT("NOTFOUND_DY:");
			const MERDY::notfound_dy& notfound_dy(obj);
			const uint64_t& key = notfound_dy.get<1>();
			const address& org = notfound_dy.get<2>();
			DEBUG_OUT("for %lu\n",key);
			
			mp::sync< std::unordered_multimap<uint64_t, get_fwd_t > >::ref send_fwd_r(send_fwd);
			std::unordered_multimap<uint64_t, get_fwd_t>::iterator it = send_fwd_r->find(key);
			
			if(it != send_fwd_r->end()){
				// read repair
				const MERDY::put_dy* put_dy = z->allocate<MERDY::put_dy>(OP::PUT_DY, key, it->second.get_vcvalue(), address(settings.myip,settings.myport));
				tuple_send_async(put_dy, org, &sockets, z);
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
			const msgpack::type::tuple<int, std::set<address> >& update_mer_hub(obj);
			const std::set<address>& tmp_mer_hub = update_mer_hub.get<1>();
			std::set<address>::iterator it = tmp_mer_hub.begin();
			break;
		}
		case OP::OK_SET_ATTR:{ // op, attr_name
			DEBUG_OUT("OK_SET_ATTR:");
			//const MERDY::ok_set_attr ok_set_attr(obj);
			//const std::string& name = ok_set_attr.get<1>();
			//const int& identifier = ok_set_attr.get<2>();
			
			break;
		}
		case OP::OK_GET_ATTR:{
			DEBUG_OUT("OK_GET_ATTR");
			const MERDY::ok_get_range& ok_get_attr(obj);
			const std::string& name = ok_get_attr.get<1>();
			const int& identifier = ok_get_attr.get<2>();
			const std::list<mercury_kvp>& ans = ok_get_attr.get<3>();
			//*
			std::list<mercury_kvp> unconst_ans = ans;
			
			mp::sync< std::unordered_map<mercury_request_fowards, std::shared_ptr<sql_select_workingset>, mercury_request_fowards_hash> >::ref suspending_selects_r(suspending_selects);
			std::unordered_map<mercury_request_fowards, std::shared_ptr<sql_select_workingset>,mercury_request_fowards>::iterator req_it = suspending_selects_r->find(mercury_request_fowards(name,identifier));
			if(req_it == suspending_selects_r->end()){
				break;
			}
			req_it->second->statements.find(mercury_request_fowards(name,identifier))->second->factor_.hashes.merge(unconst_ans);
			req_it->second->statements.find(mercury_request_fowards(name,identifier))->second->factor_.resolved_flag = true;
			suspending_selects_r->erase(req_it);
			break;
		}
		case OP::OK_GET_RANGE:{ 
			DEBUG_OUT("OK_GET_RANGE:");
			const MERDY::ok_get_range& ok_get_range(obj);
			const std::string& name = ok_get_range.get<1>();
			const int& identifier = ok_get_range.get<2>();
			const std::list<mercury_kvp>& ans = ok_get_range.get<3>();
			std::list<mercury_kvp> unconst_ans = ans;
			unconst_ans.sort();
			
			/*
			std::list<mercury_kvp>::const_iterator ans_it = ans.begin();
			while(ans_it != ans.end()){
				DEBUG(ans_it->dump());
				DEBUG_OUT(" ");
				++ans_it;
			}
			//*/
			DEBUG_OUT("in range %lu items.\n",ans.size());
			{
				mp::sync< std::unordered_map<mercury_request_fowards, std::shared_ptr<sql_select_workingset>, mercury_request_fowards_hash> >::ref 
					suspending_selects_r(suspending_selects);
				std::unordered_map<mercury_request_fowards, std::shared_ptr<sql_select_workingset>,mercury_request_fowards>::iterator req_it
					= suspending_selects_r->find(mercury_request_fowards(name,identifier));
				if(req_it == suspending_selects_r->end()){
					break;
				}
				req_it->second->statements.find(mercury_request_fowards(name,identifier))->second->factor_.hashes.merge(unconst_ans);
				req_it->second->statements.find(mercury_request_fowards(name,identifier))->second->factor_.resolved_flag = true;

				std::shared_ptr<sql_select_workingset> delay_erase = req_it->second;
				suspending_selects_r->erase(req_it);
				DEBUG_OUT("rest:[%lu]\n",suspending_selects_r->size());
				suspending_selects_r.reset();
			}
			break;
		}
		case OP::OK_TELLME_RANGE:{
			DEBUG_OUT("OK_TELLME_RANGE:");
			const msgpack::type::tuple<int, std::string>& ok_tellme_range(obj);
			const std::string& name = ok_tellme_range.get<1>();
			mp::sync< std::unordered_multimap<std::string, mercury_instance> >::ref mer_node_r(mer_node);
			std::unordered_multimap<std::string, mercury_instance>::iterator it = mer_node_r->find(name);
			
			break;
		}
		case OP::NG_TELLME_RANGE:{
			DEBUG_OUT("NG_TELLME_RANGE:");
			const msgpack::type::tuple<int, std::string>& ng_tellme_range(obj);
			const std::string& name = ng_tellme_range.get<1>();
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
			//exit(0);
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
		const MERDY::add_me_proxy add_me_proxy(OP::ADD_ME_PROXY,address(settings.myip,settings.myport));
		tuple_send(add_me_proxy,address(settings.masterip,settings.masterport),&sockets);
		const MERDY::tellme_hashes tellme_hashes(OP::TELLME_HASHES,address(settings.myip,settings.myport));
		tuple_send(tellme_hashes,address(settings.masterip,settings.masterport),&sockets);
	}
	
	// mpio start
	lo.run(4);
} 
