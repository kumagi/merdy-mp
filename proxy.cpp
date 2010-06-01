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

#include <boost/program_options.hpp>
#include <unordered_map>

static const char interrupt[] = {-1,-12,-1,-3,6};

static struct settings{
	int verbose;
	std::string interface;
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
			DEBUG_OUT("%llu->",(unsigned long long)it->first);
			DEBUG(it->second.dump());
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

mp::sync< std::unordered_multimap<std::string, std::pair<attr, MERDY::get_attr> > > suspended_select_get_attr;
mp::sync< std::unordered_multimap<std::string, std::pair<attr, MERDY::get_range> > > suspended_select_get_range;

template<typename tuple>
bool mercury_send(const std::string& name, const attr& target, const tuple& t){
	mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
	std::unordered_map<std::string,std::map<attr_range,address> >::iterator hub_it = mer_hubs_r->find(name);
	if(hub_it != mer_hubs_r->end()){
		std::map<attr_range,address>::iterator range_it = hub_it->second.begin();
		while(range_it != hub_it->second.end()){
			if(range_it->first.contain(target)){
				tuple_send(t,range_it->second);
				return true;
			}
			++range_it;
		}
		if(range_it == hub_it->second.end()){
			DEBUG_OUT("cannot to send\n");
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

struct mercury_request_fowards{
public:
	const std::string name;
	const int identifier;
	mercury_request_fowards(const std::string& _name, const int& _identifier)
		:name(_name),identifier(_identifier){}
	mercury_request_fowards(const mercury_request_fowards& org)
		:name(org.name),identifier(org.identifier){}
	bool operator<(const mercury_request_fowards& rhs)const{
		if(identifier < rhs.identifier)return true;
		else if(rhs.identifier < identifier) return false;
		return name<rhs.name;
	}
	bool operator==(const mercury_request_fowards& rhs)const{
		return name == rhs.name && identifier == rhs.identifier;
	}
	void dump()const{
		fprintf(stderr,"[%s %d] ",name.c_str(),identifier);
	}
};
class mercury_request_fowards_hash{
public:
	std::size_t operator()(const mercury_request_fowards& o)const{
		return hash32(o.name) ^ o.identifier;
	}
};
class sql_answer{
public:
	const int org;
	std::string buff;
	sql_answer(const int& _org, const std::string& _buff)
		:org(_org),buff(_buff){}
	sql_answer(const sql_answer& _org)
		:org(_org.org),buff(_org.buff){}

	~sql_answer(){
		write(org,buff.data(),buff.length());
	}
private:
	sql_answer();
};

class sql_insert_workingset{
public:
	std::list<std::string> attr_names;
	std::list<attr> attr_values;
	std::unordered_map<std::string,attr> tuple;
	uint64_t hashed_tuple;
	sql_insert_workingset(void)
		:attr_names(),attr_values(),tuple(),hashed_tuple(0){};
	
	std::string execute(std::unordered_multimap<mercury_request_fowards, std::shared_ptr<sql_answer>, mercury_request_fowards_hash>* maps,std::shared_ptr<sql_answer>* ans){
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
			
			if(maps != NULL){
				maps->insert(std::pair<mercury_request_fowards, std::shared_ptr<sql_answer> >(mercury_request_fowards(*names_it,mercury_query_identifier), *ans));
			}
			std::unordered_map<std::string,std::map<attr_range,address> >::iterator hub_map_it;
			hub_map_it = mer_hubs_r->find(*names_it); // it must find
			assert(hub_map_it != mer_hubs_r->end());
			
			std::map<attr_range,address>& target_hub = hub_map_it->second;
			std::map<attr_range,address>::iterator hub_it = target_hub.begin();
			while(hub_it != target_hub.end()){
				if(hub_it->first.contain(*values_it)){
					tuple_send(set_attr, hub_it->second);
					break;
				}
				++hub_it;
			}
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


namespace select_where{// ------------------select_where------------------
class binary;
class unary;
class statement{
public:
	std::list<mercury_kvp> hashes;
	virtual bool execute(const std::string& _name, const int _identifier,const std::list<mercury_kvp>& _list) = 0;
	virtual void resolve() = 0;
	virtual std::list<mercury_request_fowards> get_unsolved()const = 0;
	virtual void dump()const = 0;
	virtual bool is_resolved() const = 0;
	virtual ~statement() = 0;
	statement():hashes(){}
};
statement::~statement(){};

class node: public statement{
public:
	const std::string name;
	const int identifier;
	const int op;
	const attr cond;
	node(const std::string& _name, const int _identifier, const int& _op, const attr& _cond)
		:name(_name),identifier(_identifier),op(_op),cond(_cond){};
	~node(){};
	void resolve(){
		if(!this->hashes.empty()){
			return;
		}
		if(op == sqlparser::sql::op_eq){
			std::list<attr> request;
			request.push_back(cond);
			MERDY::get_attr get_attr(OP::GET_ATTR,name, identifier,request,address(settings.myip,settings.myport));
			if(!mercury_send(name, cond,get_attr)){
				mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_attr> > >::ref suspended_select_get_attr_r(suspended_select_get_attr);
				suspended_select_get_attr_r->insert(std::pair<std::string, std::pair<attr,MERDY::get_attr> >(name, std::pair<attr,MERDY::get_attr>(cond, get_attr)));	// 
			}
		}else if(op == sqlparser::sql::op_gt){
			MERDY::get_range get_range(OP::GET_RANGE,name, identifier, attr_range(cond,attr()), address(settings.myip,settings.myport));
			if(!mercury_send(name, cond,get_range)){
				mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> > >::ref suspended_select_get_range_r(suspended_select_get_range);
				suspended_select_get_range_r->insert(std::pair<std::string, std::pair<attr,MERDY::get_range> >(name, std::pair<attr,MERDY::get_range>(cond, get_range)));
			}
		}else if(op == sqlparser::sql::op_lt){
			MERDY::get_range get_range(OP::GET_RANGE,name, identifier, attr_range(attr(),cond), address(settings.myip,settings.myport));
			if(!mercury_send(name, cond,get_range)){
				mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> > >::ref suspended_select_get_range_r(suspended_select_get_range);
				suspended_select_get_range_r->insert(std::pair<std::string, std::pair<attr,MERDY::get_range> >(name, std::pair<attr,MERDY::get_range>(cond, get_range)));
			}
		}else{
			assert(!"unsupported operation");
		}
	}
	bool is_resolved()const{
		return !this->hashes.empty();
	}
	std::list<mercury_request_fowards> get_unsolved()const {
		std::list<mercury_request_fowards> unsolved;
		if(this->hashes.empty()){
			unsolved.push_back(mercury_request_fowards(name,identifier));
		}
		return unsolved;
	}
	void dump()const{
		fprintf(stderr,"id:%d[%s ", identifier,name.c_str());
		switch (op){
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
		cond.dump();
		fprintf(stderr,"] ");
		
		std::list<mercury_kvp>::const_iterator hash_it = hashes.begin();
		while(hash_it != hashes.end()){
			fprintf(stderr,"{%lu}",hash_it->get_data());
			++hash_it;
		}
		fprintf(stderr,"\n");
	}
	bool execute(const std::string& _name, const int _identifier,const std::list<mercury_kvp>& _list){
		if(!this->hashes.empty()){
			return true;
		}
		if(name == _name && identifier == _identifier){
			DEBUG_OUT("matched for %lu items %s:%d == %s:%d",_list.size(), name.c_str(),identifier, _name.c_str(),_identifier);
			std::list<mercury_kvp>::const_iterator it = _list.begin();
			while(it != _list.end()){
				DEBUG(it->dump());
				DEBUG_OUT("\n");
				++it;
			}
			this->hashes = _list;
			this->hashes.sort();
			this->hashes.unique();
			DEBUG_OUT(" exe done.\n");
			return true;
		}else{
			DEBUG_OUT("not matched for %lu items %s:%d != %s:%d",_list.size(), name.c_str(),identifier, _name.c_str(),_identifier);
		}
		DEBUG_OUT(" exe done.\n");
		return false;
	}
public:
	node();
};
class binary: public statement{
public:
	statement* lhs;
	int op;
	statement* rhs;
	binary(statement* _lhs, int _op, statement* _rhs)
		:lhs(_lhs),op(_op),rhs(_rhs){}
	~binary(){
		delete lhs;
		delete rhs;
	}
	void resolve(){
		DEBUG_OUT("! left and right resolve! !\n");
		lhs->resolve();
		rhs->resolve();
	}
	std::list<mercury_request_fowards> get_unsolved()const {
		if(!this->hashes.empty()){
			std::list<mercury_request_fowards> empty;
			return empty;
		}
		std::list<mercury_request_fowards> rhs_unsolved = rhs->get_unsolved();
		std::list<mercury_request_fowards> lhs_unsolved = lhs->get_unsolved();
		rhs_unsolved.merge(lhs_unsolved);
		return rhs_unsolved;
	}
	bool is_resolved()const{
		return !this->hashes.empty();
	}
	void dump()const{
		lhs->dump();
		switch(op){
		case sqlparser::sql::op_and:{
			fprintf(stderr," AND ");
			break;
		}
		case sqlparser::sql::op_or:{
			fprintf(stderr," OR ");
			break;
		}
		default:{
			assert(!"arien");
			break;
		}
		}
		rhs->dump();
	}
	bool execute(const std::string& _name, const int _identifier,const std::list<mercury_kvp>& _list){
		// op is [and]/[or]
		if(!this->hashes.empty()){
			return true;
		}
		
		lhs->execute(_name,_identifier,_list);
		rhs->execute(_name,_identifier,_list);
		if(!lhs->is_resolved() || !rhs->is_resolved()){
			return false;
		}
		
		DEBUG_OUT("left and right solved!\n");
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
			lhs =  NULL;
			rhs =  NULL;
			this->hashes.sort();
			std::list<mercury_kvp>::iterator hashes_it = this->hashes.begin();
			DEBUG_OUT("marged[");
			while(hashes_it != hashes.end()){
				DEBUG(hashes_it->dump());
				++hashes_it;
				DEBUG_OUT(",");
			}
			DEBUG_OUT("]\n");
			return true;
		}else if(op == sqlparser::sql::op_or){
			this->hashes.merge(lhs->hashes);
			this->hashes.merge(rhs->hashes);
			delete lhs;
			delete rhs;
			lhs = NULL;
			rhs = NULL;
			this->hashes.sort();
			this->hashes.unique();
			return true;
		}else{
			assert(!"invalid operation");
		}
		return false;
	}
};
/*
  class unary: public statement{
  public:
  int op;
  statement* target;
  std::list<uint64_t> hashes;
  bool execute(const std::string& _name, const int _identifier,const std::list<mercury_kvp>& _list){};
  };
*/

node* term(sqlparser::query* sql){
	if(sql->get().is_data()){
		std::string name = sql->get().get_data().get_string();
		int cnt = __sync_fetch_and_add(&settings.mercury_cnt,1);
		sql->next();
		int op = sql->get().get_query();
		sql->next();
		attr cond = sql->get().get_data();
		return new node(name, cnt, op, cond);
	}else{
		assert(!"operation not supported");
	}
	return NULL;
}
statement* expression(sqlparser::query* sql){
	node* lhs = term(sql);
	if(sql->next() == true){
		DEBUG(lhs->dump());
		int op = sql->get().get_query();
		sql->next();
		node* rhs = term(sql);
		return new binary(lhs,op,rhs);
	}else{
		return lhs;
	}
}
void where_parser(sqlparser::query* sql, std::list<statement*>* statements){
	statements->push_back(expression(sql));
}
}// namespace select_where


class sql_select_dynamo{
public:
	std::list<std::string> select_names;
	std::list<value_vclock> found_data;
	const int org_fd;
	sql_select_dynamo(const int& _org_fd)
		:org_fd(_org_fd){}
	~sql_select_dynamo(){
		DEBUG_OUT("-----------------\n");
		std::list<value_vclock>::iterator data_it = found_data.begin();
		std::string answer;
		while(data_it != found_data.end()){
			const std::unordered_map<std::string,attr>& data = data_it->get_value();
			std::list<std::string>::iterator names_it = select_names.begin();
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
					answer += std::string(",");
				}
			}
			++data_it;
			if(data_it != found_data.end()){
				answer += std::string("\n");
			}
		}
		DEBUG_OUT("%s\n------------------", answer.c_str());
		answer += std::string("\nselected.\n");
		write(org_fd,answer.data(),answer.length());
	}
private:
	sql_select_dynamo();
};
mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_select_dynamo> > > sql_suspended_select_dynamo;

class sql_select_workingset{
public:
	std::list<std::string> select_names;
	std::list<select_where::statement*> statements;
	const int org_fd;
	sql_select_workingset(const int& _org_fd):org_fd(_org_fd){}
	void execute(const std::string& _name,const int& _identifer,const std::list<mercury_kvp>& _list){
		statements.front()->execute(_name,_identifer,_list);
		std::list<mercury_request_fowards> rest = statements.front()->get_unsolved();
		if(rest.empty()){
			// get_attr and get_range is end
			DEBUG_OUT("start to dynamo.\n");
			const std::list<mercury_kvp>& ans = statements.front()->hashes;
			std::list<mercury_kvp>::const_iterator ans_it = ans.begin();
			std::shared_ptr<sql_select_dynamo> dynamo_ans(new sql_select_dynamo(org_fd));
			dynamo_ans->select_names.swap(select_names);
			while(ans_it != ans.end()){
				{
					mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_select_dynamo> > >::ref
						sql_suspended_select_dynamo_r(sql_suspended_select_dynamo);
					sql_suspended_select_dynamo_r->insert(std::pair<uint64_t, std::shared_ptr<sql_select_dynamo> >(ans_it->get_data(),dynamo_ans));
				}
				MERDY::get_dy get_dy(OP::GET_DY,ans_it->get_data(),address(settings.myip,settings.myport));
				const address& target = search_address(ans_it->get_data());
				tuple_send(get_dy,target);
				++ans_it;
			}
		}
	}
	void dump()const{
		std::list<std::string>::const_iterator names_it = select_names.begin();
		DEBUG_OUT("search for ...");
		while(names_it != select_names.end()){
			DEBUG_OUT("%s ",names_it->c_str());
			++names_it;
		}
		DEBUG_OUT("\n");
		std::list<select_where::statement*>::const_iterator state_it = statements.begin();
		while(state_it != statements.end()){
			(*state_it)->dump();
			++state_it;
		}
	}
private:
	sql_select_workingset();
};

mp::sync< std::multimap<mercury_request_fowards, std::shared_ptr<sql_select_workingset> > > suspending_selects;

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


mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > > sql_create_table;
mp::sync< std::unordered_multimap<mercury_request_fowards, std::shared_ptr<sql_answer>, mercury_request_fowards_hash > > sql_insert_into_mercury;
mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> > > sql_insert_into_dynamo;


class main_handler : public mp::wavy::handler {
	mp::wavy::loop* lo;
	msgpack::unpacker m_pac;
	
public:
	main_handler(int _fd,mp::wavy::loop* _lo):mp::wavy::handler(_fd),lo(_lo){
	}
	
	void event_handle(int fd, msgpack::object obj, msgpack::zone*){
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
				
				
				std::shared_ptr<sql_answer> ans(new sql_answer(fd,std::string("done.\n")));
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_data());
					const std::string& attrname = sql.get().get_data().get_string(); sql.next();
					int type;
					if(sql.get() == sqlparser::sql::data_char){
						type = attr::string;
					}else if(sql.get() == sqlparser::sql::data_int){
						type = attr::number;
					}else{
						assert(!"invalid data");
					}
					sql.next();
					{
						mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > >::ref sql_create_table_r(sql_create_table);
						sql_create_table_r->insert(std::pair<std::string,std::shared_ptr<sql_answer> >(attrname, ans));
					}
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
				{
					std::shared_ptr<sql_answer> ans(new sql_answer(fd,std::string("")));
					mp::sync< std::unordered_multimap<mercury_request_fowards, std::shared_ptr<sql_answer>, mercury_request_fowards_hash > >::ref sql_insert_into_mercury_r(sql_insert_into_mercury);
				
					{
						mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> > >::ref sql_insert_into_dynamo_r(sql_insert_into_dynamo);
						sql_insert_into_dynamo_r->insert(std::pair<uint64_t,std::shared_ptr<sql_answer> >(sets_p->hashed_tuple, ans));
					}
					// set attrs to mercury
					
					const std::string& lack_hub = sets_p->execute(&*sql_insert_into_mercury_r,&ans);
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
				}
			}else if(seg == sqlparser::sql::select){
				DEBUG_OUT("select:");
				sql.next();
				std::shared_ptr<sql_select_workingset> work_p(new sql_select_workingset(fd));
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
				
				assert(sql.get() == sqlparser::sql::from);
				if(sql.get() == sqlparser::sql::from){
					sql.next();
					assert(sql.get().is_data());
					//sql.get().get_data().get_string(); // FIXME:table name is disabled now 
					sql.next();
				}
				if(sql.get() == sqlparser::sql::where){
					sql.next();
					select_where::where_parser(&sql,&work_p->statements);
				}
				
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
				work_p->statements.front()->resolve();
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
			
			{ // answer to client
				mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > >::ref sql_create_table_r(sql_create_table);
				sql_create_table_r->erase(name);
			}
			break;
		}
		case OP::NG_CREATE_SCHEMA:{
			DEBUG_OUT("NG_CREATE_SCHEMA:");
			const MERDY::ng_create_schema ng_create_schema(obj);
			const std::string& name = ng_create_schema.get<1>();
			
			{ // answer to client
				mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_answer> > >::ref sql_create_table_r(sql_create_table);
				sql_create_table_r->erase(name);
			}
			break;
		}
		case OP::ASSIGNMENT:{
			DEBUG_OUT("ASSIGNMENT:");
			const MERDY::assignment assignment(obj);
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
			mp::sync< std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> > >::ref suspending_inserts_r(suspending_inserts);
			while(suspending_inserts_r->size() > 0){
				std::unordered_multimap<std::string, std::shared_ptr<sql_insert_workingset> >::iterator sus_it = suspending_inserts_r->find(name);
				if(sus_it == suspending_inserts_r->end()){break;}
				const std::string& lack_hub = sus_it->second->execute(NULL,NULL);
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
			
			mp::sync< std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> > >::ref suspended_select_get_range_r(suspended_select_get_range);
			if(!suspended_select_get_range_r->empty()){
				DEBUG_OUT("thehre exists suspended select query\n");
				while(1){
					std::unordered_multimap<std::string, std::pair<attr,MERDY::get_range> >::iterator get_range_it = suspended_select_get_range_r->find(name);
					
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
			
			mp::sync< std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> > >::ref 
				sql_insert_into_dynamo_r(sql_insert_into_dynamo);
			std::unordered_multimap<uint64_t, std::shared_ptr<sql_answer> >::iterator it = sql_insert_into_dynamo_r->find(key);
			it->second->buff += std::string("set ok.\n");
			sql_insert_into_dynamo_r->erase(key);
			
			DEBUG_OUT("key:%llu ok.\n",(unsigned long long)key);
			break;
		}
		case OP::FOUND_DY:{ // op, key, vcvalue, address
			DEBUG_OUT("FOUND_DY:");
			const MERDY::found_dy found_dy(obj);
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
				it->second->found_data.push_back(value);
				sql_suspended_select_dynamo_r->erase(key);
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
			const MERDY::ok_set_attr ok_set_attr(obj);
			const std::string& name = ok_set_attr.get<1>();
			const int& identifier = ok_set_attr.get<2>();
			mp::sync< std::unordered_multimap<mercury_request_fowards, std::shared_ptr<sql_answer>, mercury_request_fowards_hash> >::ref sql_insert_into_mercury_r(sql_insert_into_mercury);
			sql_insert_into_mercury_r->erase(mercury_request_fowards(name,identifier));
			DEBUG_OUT(" rest[%lu]\n",sql_insert_into_mercury_r->size());
			
			break;
		}
		case OP::OK_GET_ATTR:{
			DEBUG_OUT("OK_GET_ATTR");
			const MERDY::ok_get_range ok_get_attr(obj);
			const std::string& name = ok_get_attr.get<1>();
			const int& identifier = ok_get_attr.get<2>();
			const std::list<mercury_kvp>& ans = ok_get_attr.get<3>();
			
			mp::sync< std::multimap<mercury_request_fowards, std::shared_ptr<sql_select_workingset> > >::ref suspending_selects_r(suspending_selects);
			std::multimap<mercury_request_fowards, std::shared_ptr<sql_select_workingset> >::iterator req_it = suspending_selects_r->find(mercury_request_fowards(name,identifier));
			req_it->second->execute(name,identifier,ans);
			suspending_selects_r->erase(mercury_request_fowards(name,identifier));
			
			break;
		}
		case OP::OK_GET_RANGE:{ 
			DEBUG_OUT("OK_GET_RANGE:");
			const MERDY::ok_get_range ok_get_range(obj);
			const std::string& name = ok_get_range.get<1>();
			const int& identifier = ok_get_range.get<2>();
			const std::list<mercury_kvp>& ans = ok_get_range.get<3>();
			
			/*
			  std::list<mercury_kvp>::const_iterator ans_it = ans.begin();
			  while(ans_it != ans.end()){
			  DEBUG(ans_it->dump());
			  ++ans_it;
			  }
			//*/
			
			mp::sync< std::multimap<mercury_request_fowards, std::shared_ptr<sql_select_workingset> > >::ref suspending_selects_r(suspending_selects);
			std::multimap<mercury_request_fowards, std::shared_ptr<sql_select_workingset> >::iterator req_it = suspending_selects_r->find(mercury_request_fowards(name,identifier));
			DEBUG_OUT(" range came for %lu items.\n", ans.size());
			if(req_it != suspending_selects_r->end()){
				req_it->second->execute(name,identifier,ans);
				suspending_selects_r->erase(req_it);
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
	settings.myip = get_myip_interface(settings.interface.c_str());
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
