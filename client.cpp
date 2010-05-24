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
	unsigned short targetport;
	int myip,targetip;
	settings():verbose(10),targetport(11011),myip(get_myip()),targetip(aton("127.0.0.1")){}
}settings;


namespace po = boost::program_options;
int main(int argc, char** argv){
	srand(time(NULL));
	
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
	

	// set target.
	{
		char buff[256];
		int targetfd = create_tcpsocket();
		connect_ip_port(targetfd, settings.targetip,settings.targetport);
		for(int i=0;i<1024;++i){
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
				//fprintf(stderr,"received %d byte [%s]\n",recvlen,buff);
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
				//fprintf(stderr,"received %d byte [%s]\n",recvlen,buff);
			}
		}
	}
} 
