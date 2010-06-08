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

#include <boost/thread.hpp>
#include <boost/bind.hpp>

#include "merdy_operations.h"
#include <boost/program_options.hpp>
#include <unordered_map>


static const char interrupt[] = {-1,-12,-1,-3,6};

static struct settings{
	int verbose;
	std::string interface;
	unsigned short targetport;
	int myip,targetip;
	settings():verbose(10),targetport(11011),myip(get_myip()),targetip(aton("127.0.0.1")){}
}settings;

class file_ptr{
public:
	FILE* fp;
	file_ptr(FILE* const _fp):fp(_fp){};
	FILE* get(){
		return fp;
	}
	~file_ptr(){
		fclose(fp);
	}
};
socket_set sockets;

template<typename tuple>
inline void tuple_send(const tuple& t, const address& ad){
	msgpack::vrefbuffer vbuf;
	msgpack::pack(vbuf, t);
	const struct iovec* iov(vbuf.vector());
	sockets.writev(ad, iov, vbuf.vector_size());
}

template<typename tuple>
void proxy_tuple_send(std::pair<const tuple&,const address&> pair){
	tuple_send(pair.first,pair.second);
}

namespace po = boost::program_options;

int main(int argc, char** argv){
	srand(time(NULL));
	
	char buff[1024*1024];
	// parse options
	po::options_description opt("options");
	std::string target;
	opt.add_options()
		("help,h", "view help")
		("verbose,v", "verbose mode")
		("interface,i",po::value<std::string>(&settings.interface)->default_value("eth0"), "my interface")
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
	settings.myip = get_myip_interface2(settings.interface.c_str());
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
	

	std::set<address> proxies;
	// set target.
	{
		int targetfd = create_tcpsocket();
		connect_ip_port(targetfd, settings.targetip,settings.targetport);
		
		const MERDY::tellme_proxy tellme_proxy(OP::TELLME_PROXY);
		fprintf(stderr,"tellme proxy\n");
		tuple_send(tellme_proxy, address(settings.targetip,settings.targetport));
		int masterfd = sockets.get_socket(address(settings.targetip,settings.targetport));
		
		msgpack::unpacker pac;
		pac.reserve_buffer(1024);
		int readlen = read(masterfd,pac.buffer(),pac.buffer_capacity());
		pac.buffer_consumed(readlen);
		if(pac.execute()){
			msgpack::object msg = pac.data();
			mp::shared_ptr<msgpack::zone> z(pac.release_zone());
			pac.reset();
			const MERDY::ok_tellme_proxy ok_tellme_proxy(msg);
			proxies = ok_tellme_proxy.get<1>();
			
			std::set<address>::iterator it = proxies.begin();
			while(it != proxies.end()){
				it->dump();
				++it;
			}
			fprintf(stderr,"%lu proxy\n",proxies.size());
		}else{
			fprintf(stderr,"illegal responce from master node\n");
			exit(1);
		}
		{
			
			std::set<address>::iterator proxy_it = proxies.begin();
			file_ptr fp(fopen("testdata.txt","r"));
			
			{
				int fd = sockets.get_socket(*proxy_it);
				sprintf(buff,"CREATE TABLE merdy(name char, age int, gender int,prace int, score int, height int)\n");
				std::string create = std::string(buff);
				const msgpack::type::tuple<int,std::string> create_table(OP::DO_SQL,create);
				tuple_send(create_table, *proxy_it);
				read(fd,buff,1024);
			}
			for(int i=0;i<10000;i++){
				sprintf(buff,"INSERT INTO merdy(name,age,gender,prace,score,height)values(fuga,%d,%d,%d,%d,%d)\n",rand()%100,rand()%2,rand()%1000,rand()%300,rand()%2);
				std::string sql = std::string(buff);
				const msgpack::type::tuple<int,std::string> do_sql(OP::DO_SQL,sql);
				tuple_send(do_sql,*proxy_it);
				char buff[1024];
				
				int fd = sockets.get_socket(*proxy_it);
				int readsize = read(fd, buff, 1024);
				buff[readsize] = '\0';
				++proxy_it;
				if(proxy_it == proxies.end()){
					proxy_it = proxies.begin();
				}
				//fprintf(stderr,"%d,%s \n%s\n----\n",i,sql.c_str(),buff);
			}
			fprintf(stderr,"set done.\n");
			for(int i=0;i<100000;i++){
				
				sprintf(buff,"SELECT age,gender,score,height FROM merdy WHERE gender = 0 & (age < %d | age > %d) & score > 150 \n",rand()%50,(rand()%50)+50);
				std::string sql = std::string(buff);
				const msgpack::type::tuple<int,std::string> do_sql(OP::DO_SQL,sql);
				
				tuple_send(do_sql, *proxy_it); 
				int fd = sockets.get_socket(*proxy_it);
				
				int readsize = read(fd, buff, 1024*1024*1024);
				buff[readsize < 1024 ? readsize : 1024] = '\0';
				//fprintf(stderr,"%d,%s \n%s\n----\n",i,sql.c_str(),buff);
				if(i%100 == 0){
					fprintf(stderr,"%s \n%s\n----\n No.%d query start ",sql.c_str(),buff,i);
				}
				++proxy_it;
				if(proxy_it == proxies.end()){
					proxy_it = proxies.begin();
				}
			}
		}
	}
}
