
#include <stdlib.h>
#include <mp/wavy.h>
#include <unordered_set>
#include <msgpack.hpp>
#include "hash64.h"
#include "hash32.h"
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
};
std::size_t hash_value(const fwd_wait& o){
	return hash_value(o);
}

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
	value_vclock(const std::string& _value, int _clock=0):value(_value),clock(_clock){}
	value_vclock(const value_vclock& org):value(org.value),clock(org.clock){}
	
	bool update(const std::string& _value, unsigned int _clock){
		if(_clock > clock){
			value = _value;
			return true;
		}
		return false;
	}
	bool update(const value_vclock& newitem){
		return update(newitem.value, newitem.clock);
	}
	void update(const std::string& _value){
		value = _value;
		clock++;
		return;
	}
	
	MSGPACK_DEFINE(value, clock); // serialize and deserialize ok
private:
	value_vclock& operator=(const value_vclock&);
};

class get_fwd_t{
	int counter;
	value_vclock value_vc;
	const address origin;
public:
	get_fwd_t(const value_vclock& _value_vc, const address& _origin):counter(0),value_vc(_value_vc),origin(_origin){}
	bool update(const value_vclock newitem){
		return value_vc.update(newitem);
	}
};

std::unordered_multimap<fwd_wait, address> coordinate_fwd; // coordinate flag
std::unordered_multimap<std::string, std::pair<int,address> > put_fwd; // counter and origin address
std::unordered_multimap<fwd_wait, get_fwd_t> get_fwd; // counter and value with origin address

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
			// search coordinator, and forward
			msgpack::type::tuple<int, std::string, std::string, address> out(obj);
			const std::string& key = out.get<1>();
			const std::string& value = out.get<2>();
			const address origin = out.get<3>();
			uint64_t hash = hash_value(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			const address& coordinator = it->second;
			
			msgpack::type::tuple<int, std::string, std::string, address> mes((int)OP::SET_COORDINATE, key, value, address(settings.myip,settings.myport));
			msgpack::vrefbuffer vbuf;
			msgpack::pack(vbuf, mes);
			const struct iovec* iov(vbuf.vector());
			sockets.writev(coordinator, iov, vbuf.vector_size());
			
			coordinate_fwd.insert(std::pair<fwd_wait,address>(fwd_wait((coordinator),key),origin));
			break;
		}
		case OP::OK_SET_DY:{// op, key
			// responce for fowarding
			msgpack::type::tuple<int, std::string> out(obj);
			const std::string key = out.get<1>();
			const address& org = sockets.get_address(fd);
			
			std::unordered_multimap<fwd_wait,address>::iterator it = coordinate_fwd.find(fwd_wait(org,key));
			//msgpack::type::tuple<int, std::string>((int)OP::);
			
			break;
		}
		case OP::SET_COORDINATE:{// op, key, value, address
			msgpack::type::tuple<int, std::string, std::string, address> out(obj);
			const std::string& key = out.get<1>();
			const std::string& value = out.get<2>();
			const address& org = out.get<3>();
			put_fwd.insert(std::pair<std::string, std::pair<int, address> >(key, std::pair<int, address>(0,org)));
			
			uint64_t hash = hash_value(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			for(int i=DY::NUM; i>0; --i){		
				const address& target = it->second;
				msgpack::type::tuple<int, std::string, std::string, address> put_dy((int)OP::PUT_DY, key, value, address(settings.myip,settings.myport));
				tuple_send(put_dy, target);
				++it;
			}
			break;
		}
		case OP::PUT_DY:{// op, key, value, origin address
			// store data, only coordinator can send this message
			msgpack::type::tuple<int, std::string, std::string, address> out(obj);
			const std::string& key = out.get<1>();
			const std::string& value = out.get<2>();
			const address& ad = out.get<3>();

			std::unordered_map<std::string, value_vclock>::iterator it = key_value.find(key);
			if(it == key_value.end()){
				// new insert
				key_value.insert(std::pair<std::string, value_vclock>(key, value_vclock(value)));
			}else{
				it->second.update(value);
			}
			
			msgpack::type::tuple<int, std::string> msg((int)OP::OK_PUT_DY, key);
			tuple_send(msg,ad);
			break;
		}
		case OP::OK_PUT_DY:{// op, key
			// ack for PUT_DY, only coordinator should receives it
			msgpack::type::tuple<int, std::string> out(obj);
			const std::string& key = out.get<1>();
			std::unordered_multimap<std::string, std::pair<int, address> >::iterator it = put_fwd.find(key);
			++(it->second.first);
			if(it->second.first > DY::WRITE){
				// write ok
				fprintf(stderr,"put key:[%s] ok\n",key.c_str());
				msgpack::type::tuple<int, std::string> set_ok_coordinate((int)OP::OK_SET_COORDINATE, key);
				tuple_send(set_ok_coordinate,it->second.second);
				put_fwd.erase(it);
			}
			break;
		}
		case OP::GET_DY:{
			msgpack::type::tuple<int, std::string> get_dy(obj);
			const std::string& key = get_dy.get<1>();
			
			uint64_t hash = hash_value(key);
			hash &= ~((1<<8)-1);
			std::map<uint64_t,address>::const_iterator it = dy_hash.upper_bound(hash);
			
			
			break;
		}
		case OP::FOUND_DY:{

			break;
		}
		case OP::DEL_DY:{
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
