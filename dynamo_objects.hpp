
#include <msgpack.hpp>

// wait fowarding list
class fwd_wait{
public:
	const address target; // sended
	const uint64_t ident; // forward identifier
	
	fwd_wait(const address& _target, const uint64_t& _ident)
			:target(_target),ident(_ident){}
	bool operator==(const fwd_wait& rhs)const{
		return target == rhs.target && ident == rhs.ident;
	}
	unsigned int size()const{
		return sizeof(address) + sizeof(uint64_t);
	}
	void dump(void)const{
		target.dump();
		fprintf(stderr,"#%lu#\n",ident);
	}
};
class fwd_hash{
public:
	std::size_t operator()(const fwd_wait& o)const{
		return o.target.hash32() + hash32(o.ident);
	}
};

namespace DY{
enum dynamo_param{
	// R + W > N 
	NUM = 3,
	READ = 2,
	WRITE = 2,
};
}

class value_vclock{
	std::list<attr> value;
	unsigned int clock;
public:
	value_vclock():value(),clock(0){}
	value_vclock(const std::list<attr>& _value, int _clock=1):value(_value),clock(_clock){}
	value_vclock(const value_vclock& org):value(org.value),clock(org.clock){}
	
	int update(const std::list<attr>& _value, unsigned int _clock){
		if(clock < _clock){
			value = _value;
			clock = _clock;
			return 1;
		}else if(clock == _clock){
			return 0;
		}else{
			return -1;
		}
	}
	int update(const value_vclock& newitem){
		return update(newitem.value, newitem.clock);
	}
	void update(const std::list<attr>& _value){
		value = _value;
		clock++;
		return;
	}
	unsigned int get_clock(void)const{
		return clock;
	}
	/*
	  const char* c_str(void)const{
	  return value.c_str();
	  }
	*/
	const std::list<attr>& get_value(void)const{
		return value;
	}
	void dump(void)const{
		//std::cout << value;
		for(std::list<attr>::const_iterator it=value.begin();it != value.end(); ++it){
			//it->dump();
		}
		fprintf(stderr, "#%d ",clock);
	}
	MSGPACK_DEFINE(value, clock); // serialize and deserialize ok
private:
	value_vclock& operator=(const value_vclock&);
};

struct get_fwd_t{
	int counter;
	value_vclock value_vc;
	const address org;
public:
	get_fwd_t(const value_vclock& _value_vc, const address& _org):counter(0),value_vc(_value_vc),org(_org){}
	bool update(const value_vclock& newitem){
		++counter;
 		return value_vc.update(newitem);
	}
	inline bool count_eq(const int reads)const{
		return counter == reads;
	}
	inline const address& get_fd(void)const{
		return org;
	}
	inline int get_cnt()const{
		return counter;
	}
	inline const std::list<attr>& get_value(void)const{
		return value_vc.get_value();
	}
	inline const value_vclock& get_vcvalue(void)const{
		return value_vc;
	}
	inline void dump(void)const{
		fprintf(stderr,"origin:");
		org.dump();
		value_vc.dump();
		fprintf(stderr," cnt:%d ",counter);
	}
};
