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
	pthread_cond_t hub_cond_waiting;
	settings():verbose(10),myport(11011),masterport(11011),myip(get_myip()),masterip(aton("127.0.0.1")),i_am_master(1){}
}settings;

std::set<address> dynamo_nodes;
std::set<address> merdy_nodes;
std::map<uint64_t,address> dy_hash;
address search_address(uint64_t key){
	assert(!dy_hash.empty());
	std::map<uint64_t,address>::const_iterator hash_it = dy_hash.lower_bound(key);
	if(hash_it == dy_hash.end()){
		hash_it == dy_hash.begin();
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
				}
			}else if(seg == sqlparser::sql::insert){
				sql.next();
				assert(sql.get() == sqlparser::sql::into); sql.next();
				assert(sql.get().is_data());
				//const std::string& tablename = sql.get().get_data().get_string();
				sql.next();
				assert(sql.get() == sqlparser::sql::left_brac); sql.next();
				
				std::list<std::string> attr_names;
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_data()); 
					attr_names.push_back(sql.get().get_data().get_string());sql.next();
					
					DEBUG_OUT("parse-name\n");
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}
				}
				sql.next();
				assert(sql.get() == sqlparser::sql::values);sql.next();
				assert(sql.get() == sqlparser::sql::left_brac);sql.next();
				std::list<attr> attr_values;
				uint64_t hashed_tuple = 0;
				while(!(sql.get() == sqlparser::sql::right_brac)){
					assert(sql.get().is_data());
					attr_values.push_back(sql.get().get_data());
					hashed_tuple ^= sql.get().get_data().hash64();sql.next();
					
					DEBUG_OUT("parse-data\n");
					if(sql.get() == sqlparser::sql::comma){
						sql.next();
					}else{
						assert(sql.get() == sqlparser::sql::right_brac || sql.get().is_data());
					}
				}
				std::list<std::string>::iterator name_it = attr_names.begin();
				std::list<attr>::iterator value_it = attr_values.begin();
				assert(attr_names.size() == attr_values.size());
				
				const MERDY::set_dy set_dy(OP::SET_DY, hashed_tuple, attr_values,address(settings.myip,settings.myport));
				if(dy_hash.empty()){
					while(1){
						const MERDY::
						
					}
				}
				tuple_send(set_dy, search_address(hashed_tuple));
				while(name_it != attr_names.end()){
					int mercury_query_identifier = __sync_fetch_and_add(&settings.mercury_cnt,1);
					std::list<mercury_kvp> valuelist;
					valuelist.push_back(mercury_kvp(*value_it, hashed_tuple));
					const MERDY::set_attr set_attr(OP::SET_ATTR, *name_it, mercury_query_identifier,valuelist,address(settings.myip,settings.myport));
					
					std::unordered_map<std::string,std::map<attr_range,address> >::iterator hub_map_it;
					while(1){
						mp::sync< std::unordered_map<std::string,std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
						hub_map_it = mer_hubs_r->find(*name_it);
						if(hub_map_it == mer_hubs_r->end()){ // if the hub information doesnt exists
							const MERDY::tellme_assign tellme_assign(OP::TELLME_ASSIGN, *name_it, address(settings.myip,settings.myport));
							tuple_send(tellme_assign,address(settings.masterip,settings.masterport));
							// wait for hub receving : FIXME
							mp::pthread_mutex& hub_lock = mer_hubs_r.get_mutex();
							pthread_cond_wait(&settings.hub_cond_waiting,hub_lock.get());
							continue;
						}
						break;
					}
					std::map<attr_range,address>& target_hub = hub_map_it->second; // it may be dangerous timing
					std::map<attr_range,address>::iterator hub_it = target_hub.begin();
					while(hub_it != target_hub.end()){
						if(hub_it->first.contain(*value_it) || hub_it->first.is_invalid()){
							tuple_send(set_attr, hub_it->second);
							break;
						}
						++hub_it;
					}
					DEBUG_OUT("send..\n");
					++name_it; ++value_it;
				}
				DEBUG_OUT("done.\n");
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
			DEBUG_OUT("%s\n",name.c_str());
			break;
		}
		case OP::ASSIGNMENT:{
			DEBUG_OUT("ASSIGNMENT:");
			const MERDY::assignment assignment(obj);
			const std::string& name = assignment.get<1>();
			const std::map<attr_range,address>& hub_nodes = assignment.get<2>();
			mp::sync< std::unordered_map<std::string, std::map<attr_range,address> > >::ref mer_hubs_r(mer_hubs);
			std::unordered_map<std::string, std::map<attr_range,address> >::iterator hub_it = mer_hubs_r->find(name);
			if(hub_it == mer_hubs_r->end()){
				std::map<attr_range,address> newhub;
				std::map<attr_range,address>::const_iterator nodes = hub_nodes.begin();
				mer_hubs_r->insert(std::pair<std::string, std::map<attr_range,address> >(name, hub_nodes));
				
				pthread_cond_broadcast(&settings.hub_cond_waiting);
			}else{
				
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
			while(it != tmp_dy_hash.end()){
				dy_hash.insert(*it);
				++it;
			}
			DEBUG_OUT("%d hash received\n",(int)tmp_dy_hash.size());
			break;
		}
		case OP::OK_SET_DY:{// op, key, address
			DEBUG_OUT("OK_SET_DY:");
			// responce for fowarding
			MERDY::ok_set_dy ok_set_dy(obj);
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
	
	// mpio start
	lo.run(4);
} 
