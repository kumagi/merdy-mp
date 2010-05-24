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
	unsigned short myport,targetport;
	int myip,targetip;
	settings():verbose(10),myport(11411),targetport(11011),myip(get_myip()),targetip(aton("127.0.0.1")){}
}settings;


socket_set sockets;
mp::sync< std::map<uint64_t,address> > dy_hash;

mp::sync< std::map<std::string,std::list<address> > > mercury_assign;
volatile int hash_flag = 0;
volatile int set_flag = 0;

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
			// proxy operations
			/* ---------------------- */
		case OP::OK_CREATE_SCHEMA:{
			DEBUG_OUT("OK_CREATE_SCHEMA:\n");
			break;
		}
		case OP::UPDATE_HASHES:{
			DEBUG_OUT("UPDATE_HASHES:");
			const MERDY::update_hashes update_hashes(obj);
			const std::map<uint64_t,address>& tmp_dy_hash = update_hashes.get<1>();
			std::map<uint64_t,address>::const_iterator it = tmp_dy_hash.begin();
			
			mp::sync<std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
			while(it != tmp_dy_hash.end()){
				dy_hash_r->insert(*it);
				++it;
			}
			DEBUG_OUT("%d hash received\n",(int)tmp_dy_hash.size());
			hash_flag = 1;
			break;
		}
		case OP::ASSIGNMENT:{
			DEBUG_OUT("ASSIGNMENT:");
			MERDY::assignment assignment(obj);
			std::string& name = assignment.get<1>();
			std::list<address>& assign = assignment.get<2>();
			
			mp::sync< std::map<std::string,std::list<address> > >::ref mercury_assign_r(mercury_assign);
			mercury_assign_r->insert(std::pair<std::string, std::list<address> >(name, assign));
			
			DEBUG_OUT("ok\n");
			break;
		}
		case OP::NO_ASSIGNMENT:{
			DEBUG_OUT("NO_ASSIGNMENT:");
			const MERDY::no_assignment no_assignment(obj);
			const std::string& name = no_assignment.get<1>();
			DEBUG_OUT("for %s\n",name.c_str());
			break;
		}
		case OP::OK_SET_DY:{// op, key, address
			static int num = 1020;
			DEBUG_OUT("OK_SET_DY:%d ",num);
			// responce for fowarding
			lock_mut lock(&mut);
			num--;
			if(num == 0){
				set_flag = 1;
			}
			break;
		}
		case OP::FOUND_DY:{ // op, key, vcvalue, address
			static int found = 1020;
			DEBUG_OUT("FOUND_DY:");
			const MERDY::found_dy found_dy(obj);
			const uint64_t& key = found_dy.get<1>();
			const value_vclock& value = found_dy.get<2>();
			const address& org = found_dy.get<3>();
			DEBUG_OUT("%lu -> %s ",key,value.get_string().c_str());
			if(hash64(value.get_string()) == key){
				DEBUG_OUT("ok\n");
			}else{
				DEBUG_OUT("ng\n");
			}
			
			lock_mut lock(&mut);
			found--;
			if(found == 0){
				DEBUG_OUT("done.");
			}
			break;
		}
		case OP::OK_SET_ATTR:{
			DEBUG_OUT("OK_SET_ATTR:");
			set_flag = 1;
			break;
		}
		case OP::OK_GET_RANGE:{
			DEBUG_OUT("OK_GET_RANGE:");
			const MERDY::ok_get_range ok_get_range(obj);
			const std::string& name = ok_get_range.get<1>();
			//const int identifer = ok_get_range.get<2>();
			const std::list<mercury_kvp>& kvps = ok_get_range.get<3>();
			
			fprintf(stderr," for %s ",name.c_str());
			for(std::list<mercury_kvp>::const_iterator it = kvps.begin(); it != kvps.end(); ++it){
				fprintf(stderr,"%s -> %lu\n",it->get_attr().get_string().c_str(),it->get_data());
			}
			fprintf(stderr,"done.\n");
			break;
		}
		case OP::OK_GET_ATTR:{
			DEBUG_OUT("OK_GET_ATTR:");
			const MERDY::ok_get_attr ok_get_attr(obj);
			const std::string& name = ok_get_attr.get<1>();
			//const int& identifier = ok_get_attr.get<2>();
			const std::list<mercury_kvp>& kvps = ok_get_attr.get<3>();
			//std::cout << obj << std::endl;
			fprintf(stderr," for [%s]\n",name.c_str());
			for(std::list<mercury_kvp>::const_iterator it = kvps.begin(); it != kvps.end(); ++it){
				fprintf(stderr,"%s -> %lu\n",it->get_attr().get_string().c_str(),it->get_data());
			}
			fprintf(stderr,"done.\n");
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

void* testbench(void*){
	{// set_dy:{ // op, attr_name, list<mercury_kvp>, address
		MERDY::tellme_hashes tellme_hashes(OP::TELLME_HASHES, address(settings.myip,settings.myport));
		tuple_send(tellme_hashes,address(settings.targetip,settings.targetport));
		while(!hash_flag);
		
		DEBUG_OUT("start to save\n");
		mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
		std::map<uint64_t,address>::iterator dy_target = dy_hash_r->begin();
		for(int i=0;i<1020;i++){
			char buff[256];
			sprintf(buff,"k%d",i);
			uint64_t key = hash64(std::string(buff));
			const MERDY::set_dy set_dy(OP::SET_DY,key,std::string(buff), address(settings.myip,settings.myport));
			tuple_send(set_dy,dy_target->second);
			++dy_target;
			if(dy_target == dy_hash_r->end()){
				dy_target = dy_hash_r->begin();
			}
		}
		
		while(!set_flag);
		
	}
	{
		mp::sync< std::map<uint64_t,address> >::ref dy_hash_r(dy_hash);
		std::map<uint64_t,address>::iterator dy_target = dy_hash_r->begin();
		for(int i=0;i<1020;i++){
			char buff[256];
			sprintf(buff,"k%d",i);
			uint64_t key = hash64(std::string(buff));
			const MERDY::get_dy get_dy(OP::GET_DY,key, address(settings.myip,settings.myport));
			tuple_send(get_dy,dy_target->second);
			++dy_target;
			if(dy_target == dy_hash_r->end()){
				dy_target = dy_hash_r->begin();
			}
		}
	}
	DEBUG_OUT("finish!");
	return NULL;
}

namespace po = boost::program_options;
int main(int argc, char** argv){
	srand(time(NULL));
	pthread_mutex_init(&mut,NULL);
	
	// parse options
	po::options_description opt("options");
	std::string target;
	opt.add_options()
		("help,h", "view help")
		("verbose,v", "verbose mode")
		("address,a",po::value<std::string>(&target)->default_value("127.0.0.1"), "target address")
		("tport,P",po::value<unsigned short>(&settings.targetport)->default_value(11011), "target port");
	

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
	settings.targetip = aton(target.c_str());

	
	// view options
	printf("verbose:%d\naddress:[%s]\n",
		   settings.verbose,ntoa(settings.myip));
	printf("target:[%s:%d]\n"
		   ,ntoa(settings.targetip),settings.targetport);
	
	
    if (sigignore(SIGPIPE) == -1) {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(1);
    }
	
	// init mpio
	using namespace mp::placeholders;
	mp::wavy::loop lo;
	sockets.set_wavy_loop(&lo);
	
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_port = htons(settings.myport);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	lo.listen(PF_INET, SOCK_STREAM, 0,
			  (struct sockaddr*)&addr, sizeof(addr),
			  mp::bind(&on_accepted, _1, _2, &lo));
	
	
	pthread_t test;
	pthread_create(&test,NULL,testbench,NULL);
	pthread_detach(test);
	
	lo.run(2);
	{
		
		/*
		  for(int i=0;i<4096;++i){
		  {
		  char key[64];
		  char val[64];
		  sprintf(key,"k%d",i);
		  sprintf(val,"v%d",i);
		  msgpack::type::tuple<int,std::string,std::string> set_dy((int)OP::SET_DY, std::string(key),std::string(val));
				
		  msgpack::vrefbuffer vbuf;
		  msgpack::pack(vbuf, set_dy);
		  const struct iovec* iov(vbuf.vector());
		  writev(targetfd, iov, vbuf.vector_size());
			
		  int recvlen = read(targetfd,buff,256);
		  buff[recvlen] = '\0';
		  fprintf(stderr,"received %d byte [%s]\n",recvlen,buff);
		  }
		  {
		  char key[64];
		  sprintf(key,"k%d",i);
		  msgpack::type::tuple<int,std::string> get_dy((int)OP::GET_DY, std::string(key));
		  msgpack::vrefbuffer vbuf;
		  msgpack::pack(vbuf, get_dy);
		  const struct iovec* iov(vbuf.vector());
		  writev(targetfd, iov, vbuf.vector_size());
			
		  int recvlen = read(targetfd,buff,256);
		  buff[recvlen] = '\0';
		  fprintf(stderr,"received %d byte [%s]\n",recvlen,buff);
		  }
		  }
		*/
	}
} 
