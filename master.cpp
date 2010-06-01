#include <stdint.h>
#include <stdlib.h>
#include <mp/wavy.h>
#include <unordered_set>
#include <msgpack.hpp>
#include "unordered_map.hpp"
#include "hash64.h"
#include "random64.h"
#include "tcp_wrap.h"
#include "address.hpp"
#include "sockets.hpp"
#include "debug_mode.h"
#include "merdy_operations.h"
#include <boost/program_options.hpp>
#include "mercury_objects.hpp"
#include "dynamo_objects.hpp"
#include <limits.h>


struct schema_fwd{
	const address org;
	int cnt;
	explicit schema_fwd(const address& _org):org(_org),cnt(4){}
	explicit schema_fwd(const schema_fwd& _org):org(_org.org),cnt(4){}
};

mp::sync< std::map<std::string, schema_fwd> > create_schema_fwd;

static const char interrupt[] = {-1,-12,-1,-3,6};

static struct settings{
	int verbose;
	unsigned short myport,masterport;
	int myip,masterip;
	int i_am_master;
	settings():verbose(10),myport(11011),masterport(11011),myip(get_myip()),masterip(aton("127.0.0.1")),i_am_master(1){}
}settings;

mp::sync< std::set<address> > dynamo_nodes;
mp::sync< std::set<address> > mercury_nodes;

mp::sync< std::map<std::string,std::map<attr_range,address> > > mercury_assign;

mp::sync< std::map<uint64_t,address> > dy_hash;

socket_set sockets;


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
public:
	main_handler(int _fd,mp::wavy::loop* _lo):
		mp::wavy::handler(_fd),lo(_lo){ }
	
	void event_handle(int fd, msgpack::object obj, msgpack::zone* z){
		msgpack::type::tuple<int> out(obj);
		int operation = out.get<0>();
		switch (operation){
		case OP::CREATE_SCHEMA:{
			const MERDY::create_schema create_schema(obj);
			const std::string& name = create_schema.get<1>();
			const int& type = create_schema.get<2>();
			const address& org = create_schema.get<3>();
			
			{
				mp::sync< std::map<std::string,std::map<attr_range,address> > >::ref mercury_assign_r(mercury_assign);
				if(mercury_assign_r->find(name) != mercury_assign_r->end()){
					fprintf(stderr,"error: attribute [%s] already exists.\n",name.c_str());
					
					const MERDY::ng_create_schema ng_create_schema(OP::NG_CREATE_SCHEMA, name);
					tuple_send(ng_create_schema, org);
					break;
				}
			}
			
			mp::sync< std::set<address> >::ref mercury_nodes_r(mercury_nodes);
			std::set<address>::const_iterator it = mercury_nodes_r->begin();
			int random_loop = rand()%mercury_nodes_r->size();
			while(random_loop){
				++it;
				if(it == mercury_nodes_r->end()){
					it = mercury_nodes_r->begin();
				}
				--random_loop;
			}  
			assert(it != mercury_nodes_r->end());
			
			// set first attribute
			attr_range first_range[4];
			if(type == DATA::INT){
				first_range[0] = attr_range(attr(INT_MIN),attr(INT_MAX));
			}else{
				first_range[0] = attr_range(attr(" "),attr("~~~~~~~~~~~~~~"));
			}
			
			first_range[0].cut_uphalf(&first_range[2]);
			first_range[0].cut_uphalf(&first_range[1]);
			first_range[2].cut_uphalf(&first_range[3]);
			
			
			for(int i=0;i<4;i++){
				DEBUG(first_range[i].dump());
			}
			
			// setting hub
			std::map<attr_range,address> hub;
			for(int i=0;i<4;i++){
				hub.insert(std::pair<attr_range,address>(first_range[i], *it));
				++it;
				if(it == mercury_nodes_r->end()){it = mercury_nodes_r->begin();}
			}
			std::map<attr_range,address>::const_iterator hub_it = hub.begin();
			
			mp::sync< std::map<std::string, schema_fwd> >::ref create_schema_fwd_r(create_schema_fwd);
			if(create_schema_fwd_r->find(name) != create_schema_fwd_r->end()){
				// FIXME: fail to create schema
			}else{
				create_schema_fwd_r->insert(std::pair<std::string, schema_fwd>(name, schema_fwd(org)));
				std::map<attr_range,address> assignments;
				DEBUG_OUT("\nstart to assign  ");
				for(int i=0; i<4; i++){
					const MERDY::assign_range assign_range(OP::ASSIGN_RANGE, name, hub_it->first, hub,address(settings.myip,settings.myport));
					tuple_send(assign_range, hub_it->second);
					DEBUG(hub_it->first.dump());
					DEBUG_OUT(" -> ");
					DEBUG(hub_it->second.dump());
					DEBUG_OUT("\n");
					++hub_it;

					if(it == mercury_nodes_r->end()){ it = mercury_nodes_r->begin();}
				}
				mp::sync< std::map<std::string,std::map<attr_range,address> > >::ref mercury_assign_r(mercury_assign);
				mercury_assign_r->insert(std::pair<std::string, std::map<attr_range,address> >(name,hub));
			}
			break;
		}
		case OP::OK_ASSIGN_RANGE:{
			DEBUG_OUT("OK_ASSIGN_RANGE:");
			const MERDY::ok_assign_range ok_assign_range(obj);
			const std::string& name = ok_assign_range.get<1>();
			mp::sync< std::map<std::string, schema_fwd> >::ref create_schema_fwd_r(create_schema_fwd);
			std::map<std::string, schema_fwd>::iterator it = create_schema_fwd_r->find(name);
			
			assert(it != create_schema_fwd_r->end());
			--it->second.cnt;
			if(it->second.cnt == 0){
				mp::sync< std::map<std::string,std::map<attr_range,address> > >::ref mercury_assign_r(mercury_assign);
				std::map<std::string,std::map<attr_range,address> >::iterator newhub = mercury_assign_r->find(name);
				const MERDY::ok_create_schema ok_create_schema(OP::OK_CREATE_SCHEMA, name, newhub->second);
				tuple_send(ok_create_schema, it->second.org);
				create_schema_fwd_r->erase(name);
			}
			DEBUG_OUT(" %s ok.\n",name.c_str());
			break;
		}
		case OP::NG_ASSIGN_RANGE:{
			DEBUG_OUT("NG_ASSIGN_RANGE:");
			MERDY::ng_assign_range ng_assign_range(obj);
			std::string& name = ng_assign_range.get<1>();
			DEBUG_OUT(" for %s\n", name.c_str());
			break;
		}
		case OP::DELETE_SCHEMA:{
			
			break;
		}
		case OP::SEND_DY_LIST:{
			msgpack::vrefbuffer vbuf;
			msgpack::pack(vbuf, static_cast<int>(OP::UPDATE_HASHES));
			mp::sync< std::set<address> >::ref dynamo_nodes_r(dynamo_nodes);
			msgpack::pack(vbuf, *dynamo_nodes_r);
			const struct iovec* iov = vbuf.vector();
			lo->write(fd, iov, vbuf.vector_size());
			break;
		}
		case OP::SEND_HASHES:{
			msgpack::vrefbuffer vbuf;
			msgpack::pack(vbuf, static_cast<int>(OP::UPDATE_HASHES));
			const struct iovec* iov = vbuf.vector();
			writev(fd, iov, vbuf.vector_size());
			break;
		}
		case OP::SEND_MER_LIST:{
			DEBUG_OUT("SEND_MER_LIST:");
			msgpack::vrefbuffer vbuf;
			msgpack::pack(vbuf, static_cast<int>(OP::UPDATE_MER_HUB));
			mp::sync< std::set<address> >::ref mercury_nodes_r(mercury_nodes);
			msgpack::pack(vbuf, *mercury_nodes_r);
			const struct iovec* iov = vbuf.vector();
			writev(fd, iov, vbuf.vector_size());
			break;
		}
		case OP::TELLME_HASHES:{
			DEBUG_OUT("TELLME_HASHES:");
			const MERDY::tellme_hashes tellme_hashes(obj);
			const address& org = tellme_hashes.get<1>();
			DEBUG_OUT("from ");
			DEBUG(org.dump());
			mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
			const MERDY::update_hashes update_hashes(OP::UPDATE_HASHES,*dy_hash_r);
			tuple_send(update_hashes,org);
			DEBUG_OUT("done.\n");
			break;
		}
		case OP::ADD_ME_DY:{// op, address
			DEBUG_OUT("ADD_ME_DY");
			const MERDY::add_me_dy add_me_dy(obj);
			const address& newnode = add_me_dy.get<1>();
			
			DEBUG(newnode.dump());
			
			mp::sync< std::set<address> >::ref dynamo_nodes_r(dynamo_nodes);
			dynamo_nodes_r->insert(newnode);
			
			DEBUG_OUT("dy_nodes:#%d#\n",(int)dynamo_nodes_r->size());
			
			uint64_t hashes;
			int num = (rand() % 3) + 5;
			
			mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
			while(num > 0){
				hashes = rand64();
				dy_hash_r->insert(std::pair<uint64_t,address>(hashes, newnode));
				num--;
			}
			
			std::set<address>::iterator it = dynamo_nodes_r->begin();
			const MERDY::update_hashes update_hashes((int)OP::UPDATE_HASHES,*dy_hash_r);
			
			while(it != dynamo_nodes_r->end()){
				tuple_send(update_hashes,*it);
				DEBUG_OUT("send to ");
				DEBUG(it->dump());
				++it;
			}
			break;
		}
		case OP::ADD_ME_MER:{
			DEBUG_OUT("ADD_ME_MER");
			const MERDY::add_me_mer add_me_mer(obj);
			const address& newnode = add_me_mer.get<1>();
			
			mp::sync< std::set<address> >::ref mercury_nodes_r(mercury_nodes);
			mercury_nodes_r->insert(newnode);
			
			const MERDY::ok_add_me_mer ok_add_me_mer((int)OP::OK_ADD_ME_MER, *mercury_nodes_r);
			tuple_send(ok_add_me_mer, newnode);
			break;
		}
		case OP::TELLME_ASSIGN:{
			DEBUG_OUT("TELLME_ASSIGN");
			const MERDY::tellme_assign tellme_assign(obj);
			const std::string& attrname = tellme_assign.get<1>();
			const address& org  = tellme_assign.get<2>();
			DEBUG_OUT(" from ");
			DEBUG(org.dump());
			DEBUG_OUT(" about %s \n", attrname.c_str());
			
			mp::sync< std::map<std::string,std::map<attr_range,address> > >::ref mercury_assign_r(mercury_assign);
			std::map<std::string,std::map<attr_range,address> >::iterator it = mercury_assign_r->find(attrname);
			
			if(it != mercury_assign_r->end()){
				const MERDY::assignment assignment(OP::ASSIGNMENT, attrname, it->second);
				tuple_send(assignment, org);
			}else{
				const MERDY::no_assignment no_assignment(OP::NO_ASSIGNMENT, attrname);
				tuple_send(no_assignment, org);
			}
			break;
		}
		case OP::NO_ASSIGNMENT:{
			DEBUG_OUT("NO_ASSIGNMENT");
			break;
		}
		default:{
			DEBUG_OUT("illegal message: %d\n",operation);
			assert(!"");
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
					std::auto_ptr<msgpack::zone> z( m_pac.release_zone() );
					m_pac.reset();

					e.more();  //e.next();
					
					//DEBUG(std::cout << "object received: " << msg << std::endl);
					event_handle(fd(), msg, &*z);
					return;
				}
				
				m_pac.reserve_buffer(8*1024);

				int read_len = ::read(fd(), m_pac.buffer(), m_pac.buffer_capacity());
				if(read_len <= 0) {
					if(read_len == 0) { perror("closed"); throw mp::system_error(errno, "connection closed"); }
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
	using namespace mp::placeholders;
	mp::wavy::loop lo;
	sockets.set_wavy_loop(&lo);

    if (sigignore(SIGPIPE) == -1) {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(1);
    }
	
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_port = htons(settings.myport);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	lo.listen(PF_INET, SOCK_STREAM, 0,
			  (struct sockaddr*)&addr, sizeof(addr),
			  mp::bind(&on_accepted, _1, _2, &lo));
	
	
	lo.run(2);
}
