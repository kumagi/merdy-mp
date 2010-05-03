
#include <stdlib.h>
#include <mp/wavy.h>
#include <unordered_set>
#include <msgpack.hpp>
#include "hash64.h"
#include "random64.h"
#include "tcp_wrap.h"
#include "address.hpp"
#include "sockets.hpp"
#include "merdy_operations.h"


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
class coordinate_wait{
public:
	const int fd;
	const std::string key;
	coordinate_wait(const int& _fd, const std::string& _key):fd(_fd),key(_key){}
	bool operator==(const coordinate_wait& rhs)const{
		return fd == rhs.fd && key == rhs.key;
	}
};
class set_wait{
public:
	const int fd;
	const std::string key;
	set_wait(const int& _fd, const std::string& _key):fd(_fd),key(_key){}
	bool operator==(const set_wait& rhs)const{
		return fd == rhs.fd && key == rhs.key;
	}
};
	
		
std::unordered_multimap<coordinate_wait, int> coordinate_fwd;



std::unordered_map<std::string, std::string> key_value;

void dump_hashes(){
	std::map<uint64_t,address>::iterator it = dy_hash.begin();
	while(it != dy_hash.end()){
		it->second.dump();
		it++;
	}
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
		case OP::UPDATE_HASHES:{
			msgpack::type::tuple<int,std::map<uint64_t,address> > out(obj);
			std::map<uint64_t,address> tmp_dy_hash = out.get<1>();
			std::map<uint64_t,address>::iterator it = tmp_dy_hash.begin();
			
			while(it != tmp_dy_hash.end()){
				dy_hash.insert(*it);
				
				// it->second.dump();
				++it;
			}
			break;
		}
		case OP::OK_ADD_ME_DY:{
			fprintf(stderr,"status: ok");
			break;
		}
		case OP::SET_DY:{// op, key, value, replicas
			// it searches coordinator
			msgpack::type::tuple<int, std::string, std::string, int> out(obj);
			std::string& key = out.get<1>();
			std::string& value = out.get<2>();
			const int replicas = out.get<3>();
			uint64_t hash = hash_value(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			const address& coordinator = it->second;
			
			msgpack::type::tuple<int, std::string, std::string, int> mes((int)OP::SET_COORDINATE, key, value, replicas);
			msgpack::vrefbuffer vbuf;
			msgpack::pack(vbuf, mes);
			const struct iovec* iov(vbuf.vector());
			sockets.writev(coordinator, iov, vbuf.vector_size());
			
			coordinate_fwd.insert(std::pair<coordinate_wait,int>(coordinate_wait(sockets.get_socket(coordinator),key),fd));
			break;
		}
		case OP::OK_SET_DY:{
			
			break;
		}
		case OP::PUT_DY:{
			// it stores data without any check
			
			int ans = OP::OK_PUT_DY;
			lo->write(fd, &ans, 4);
			break;
		}
		case OP::OK_PUT_DY:{
			break;
		}
		case OP::GET_DY:{
			break;
		}
		case OP::DEL_DY:{
			break;
		}
		case OP::SET_COORDINATE:{
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
					
					std::cout << "object received: " << msg << std::endl;
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
