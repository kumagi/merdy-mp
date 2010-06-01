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

namespace po = boost::program_options;
int main(int argc, char** argv){
	srand(time(NULL));
	
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
	settings.myip = get_myip_interface(settings.interface.c_str());
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
	

	// set target.
	{
		char buff[256];
		int targetfd = create_tcpsocket();
		connect_ip_port(targetfd, settings.targetip,settings.targetport);
		{
			file_ptr fp(fopen("testdata.txt","r"));
			int fd = sockets.get_socket(address(settings.targetip,settings.targetport));
			while(fgets(buff,256,fp.get())){
				std::string sql = std::string(buff);
				if(sql.data()[0] == '#')continue;
				if(sql == std::string("\n")) continue;
				const msgpack::type::tuple<int,std::string> do_sql(OP::DO_SQL,sql);
				tuple_send(do_sql,address(settings.targetip,settings.targetport));
				char buff[1024];
				int readsize = read(fd, buff, 1024);
				buff[readsize] = '\0';
				fprintf(stderr,"%s \n%s\n----\n",sql.c_str(),buff);
			}
		}
	}
}
