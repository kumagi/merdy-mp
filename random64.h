
#include <boost/random.hpp>
// init randoms
namespace{
boost::mt19937 gen(static_cast<unsigned long>(time(NULL)));
boost::uniform_smallint<> dst(0,INT_MAX);
boost::variate_generator<boost::mt19937&, boost::uniform_smallint<> > mt_rand(gen,dst);
}
inline long long rand64(void){
	long long tmp = mt_rand();
	tmp <<= 32;
	tmp |= mt_rand();
	return tmp;
}
