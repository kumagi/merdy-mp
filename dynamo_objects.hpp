
#ifndef DYNAMO_OBJECTS_HPP__
#define DYNAMO_OBJECTS_HPP__

#include <msgpack.hpp>
#include "mercury_objects.hpp"

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
		fprintf(stderr,"#%llu#\n",(unsigned long long)ident);
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
class vclock{
    struct node{
        int clock;
        address owner;
        node():clock(0),owner(){}
        explicit node(const address& _owner):clock(1),owner(_owner){}
        node(const address& _owner,const int _clock):
            clock(_clock),owner(_owner){}
        MSGPACK_DEFINE(owner,clock);
    };
    std::vector<node> vector;
public:
    vclock():vector(0){}
    vclock(int clock, const address& owner)
        :vector(0){
        vector.push_back(node(owner,clock));
    }
    bool operator<(const vclock& rhs)const{
        std::vector<node>::const_iterator lhs_it = vector.begin();
        std::vector<node>::const_iterator rhs_it = rhs.vector.begin();
        for(;lhs_it != vector.end(); ++lhs_it){
            for(;rhs_it != vector.end(); ++rhs_it){
                if(lhs_it->owner != rhs_it->owner) continue;
                if(lhs_it->clock < rhs_it->clock) return true;
            }
        }
        return false;
    }
    bool operator==(const vclock& rhs)const{
        if(vector.size() != rhs.vector.size()) return false;
        std::vector<node>::const_iterator lhs_it = vector.begin();
        std::vector<node>::const_iterator rhs_it = rhs.vector.begin();
        while(lhs_it != vector.end()){
            if(lhs_it->owner != rhs_it->owner) return false;
            if(lhs_it->clock != rhs_it->clock) return false;
            ++lhs_it;++rhs_it;
        }
        return true;
    }

    // for coordinator
    void increment(const address& target){
        for(unsigned int i=0;i<vector.size();++i){
            if(vector[i].owner == target){
                vector[i].clock++;
                return;
            }
        }
        vector.push_back(node(target,1));
    }
    vclock& operator=(const vclock& rhs){
        vector = rhs.vector;
        return *this;
    }
    void merge(const vclock& rhs){
        const int first_size = vector.size();
        for(int i=0;i<first_size;++i){
            std::vector<node>::const_iterator rhs_it = rhs.vector.begin();
            unsigned int j;
            for(j=0;j<rhs.vector.size();++j){
                if(vector[i].owner == rhs.vector[j].owner){
                    vector[i].clock = vector[j].clock > rhs.vector[j].clock
                        ? vector[i].clock
                        : rhs.vector[j].clock;
                    break;
                }
            }
            if(j == rhs.vector.size()){
                vector.push_back(rhs.vector[j]);
            }
        }
    }
    void dump()const{
        for(unsigned int i=0;i<vector.size();++i){
            fprintf(stderr,"[");
            vector[i].owner.dump();
            fprintf(stderr,":%d]",vector[i].clock);
        }
    }
    MSGPACK_DEFINE(vector);
private:
    bool exist(const address& target)const{
        for(unsigned int i=0;i<vector.size();++i){
            if(vector[i].owner == target){
                return true;
            }
        }
        return false;
    }
};

class value_vclock{
	//std::unordered_map<std::string, attr> value;
    msgpack::object value;
	vclock clock;
public:
	value_vclock():value(),clock(){}
	value_vclock(const msgpack::object& _value, const address& owner)
        :value(_value),clock(1,owner){}
	value_vclock(const value_vclock& org)
        :value(org.value),clock(org.clock){}

    // for coorinator
	void update_increment(const msgpack::object& _value,const address& target)
    {
        clock.increment(target);
        value = _value;
	}
    // for not coorinator
    void overwrite(const value_vclock& org){
        clock.merge(org.get_clock());
        value = org.value;
    }
	const vclock& get_clock(void)const{
		return clock;
	}
	const msgpack::object& get_value(void)const{
		return value;
	}
    void dump(void)const{
        std::cerr << value;
		clock.dump();
	}
    value_vclock& operator=(const value_vclock& rhs){
        value = rhs.value;
        clock.merge(rhs.clock);
        return *this;
    }
	MSGPACK_DEFINE(value, clock); // serialize and deserialize ok
};

struct get_fwd_t{
	int counter;
	std::vector<value_vclock> value_vc;
	const address org;
public:
	get_fwd_t(const address& _org):counter(0),value_vc(),org(_org){}
	void update(const value_vclock& newitem){
		++counter; // for quorum
        fprintf(stderr,"updated [%d -> %d] at (%p)", counter-1,counter,this);
 		if(!value_vc.empty()){
            value_vc.push_back(newitem);
        }else{
        }
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
	inline const std::vector<value_vclock>& get_vcvalue(void)const{
		return value_vc;
	}
	inline void dump(void)const{
		fprintf(stderr,"origin:");
		org.dump();
		for(unsigned int i = 0;i < value_vc.size(); ++i){
            value_vc[i].dump();
        }
		fprintf(stderr," cnt:%d ",counter);
	}
    bool add(const value_vclock& obj){
        
        return true;
    }
};


#endif // DYNAMO_OBJECTS_HPP__
