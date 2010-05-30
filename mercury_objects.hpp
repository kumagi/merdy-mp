#include "hash32.h"
// mercury object

std::string get_strhalf(std::string begin, std::string end){
	int conflict = -1;
	unsigned char head,tail;
	do{
		conflict++;
		head = begin.c_str()[conflict];
		tail = end.c_str()[conflict];
	}while((char)((int)head + (int)tail / 2) == begin.c_str()[conflict]);
	char middle[begin.length()];
	memcpy(middle, begin.data(), begin.length());
	middle[conflict] = (char)((int)head + (int)tail / 2);
	middle[conflict+1] = '\0';
	return std::string(middle);
}


class attr{
	int num;
	std::string str;
	int num_or_str;
public:
	enum flag{
		number = 0,
		string = 1,
		//invalid = 2,
	};
	attr():num(0),str(std::string("")),num_or_str(num){};
	explicit attr(int _num):num(_num),str(),num_or_str(number){};
	explicit attr(const std::string& _str):num(),str(_str),num_or_str(string){}
	attr(const attr& org):num(org.num),str(org.str),num_or_str(org.num_or_str){}
	
	int get_int()const{
		assert(num_or_str == number);
		return num;
	}
	std::string get_string() const{
		assert(num_or_str == string);
		return str;
	}
	void set_string(const std::string& _str){
		assert(num_or_str == string);
		str = _str;
	}
	bool is_int()const{
		return num_or_str == number;
	}
	bool is_string()const{
		return num_or_str == string;
	}
	void maximize(){
		if(num_or_str == number){
			num = INT_MAX;
		}else{
			str = std::string("~~~~~~~~~");
		}
	}
	void minimize(){
		if(num_or_str == number){
			num = INT_MIN;
		}else{
			str = std::string(" ");
		}
	}
	//bool is_invalid()const{
	//	return number == invalid;
	//}
	int get_type()const{
		return num_or_str;
	}
	void set(int _num){
		num_or_str = number;
		num = _num;
	}
	void set(std::string& _str){
		num_or_str = string;
		str = _str;
	}
	attr get_middle(const attr& rhs)const{
		// assertion: rhs is bigger than me
		if(is_int()){
			return attr((num>>1) + (rhs.num>>1));
		}else{
			return attr(get_strhalf(str, rhs.get_string()));
		}
	}
	bool operator<(const attr& rhs)const{
		//assert(num_or_str == rhs.num_or_str);
		
		if(num_or_str == number){return num < rhs.num;}
		else if(num_or_str == string){return str < rhs.str;}else{
			assert(!"invalid comparison");
			return 0;
		}
	}
	bool operator==(const attr& rhs)const{
		assert(num_or_str == rhs.num_or_str);
		if(num_or_str == number){
			return num == rhs.num;
		}else if(num_or_str == string){
			return str == rhs.str;
		}else{
			assert(!"invalid comparison");
			return 0;
		}
	}
	void operator=(const attr& rhs){
		num = rhs.num;
		str = rhs.str;
		num_or_str = rhs.num_or_str;
	}
	std::size_t hash32()const{
		return num + ::hash32(str);
	}
	std::size_t hash64()const{
		return num + ::hash64(str);
	}
	void dump()const{
		if(num_or_str == number){
			fprintf(stderr,"%d ",num);
		}else if(num_or_str == string){
			fprintf(stderr,"%s ",str.c_str());
		}else{
			fprintf(stderr,"* ");
		}
	}
	MSGPACK_DEFINE(num, str, num_or_str); // serialize and deserialize ok
};



namespace closed{
enum flag{
	closed = true,
	opened = false,
};
}
class attr_range{
	attr begin;
	attr end;
	bool left_closed;
	bool right_closed;
public:
	attr_range():begin(),end(),left_closed(true),right_closed(true){};
	attr_range(const attr_range& org):begin(org.begin),end(org.end),left_closed(org.left_closed),right_closed(org.right_closed){};
	attr_range(const attr& _begin, const attr& _end):begin(_begin),end(_end),left_closed(true),right_closed(true){
		assert(_begin < _end);
	}
	attr_range(const attr& _begin, const attr& _end, const bool _l, const bool _r):begin(_begin),end(_end),left_closed(_l),right_closed(_r){
		assert(_begin < _end);
	}
	bool operator==(const attr_range& rhs)const{
		return begin == rhs.begin && end == rhs.end && left_closed == rhs.left_closed && right_closed == rhs.right_closed;
	}
	void get_common_range(const attr_range& range, attr_range* half)const{
		if(contain(range)){
			half->begin = (begin < range.begin) ? range.begin : begin;
			half->end = (range.end < end) ? range.end : end;
			half->left_closed = (begin < range.begin) ? range.left_closed : left_closed;
			half->right_closed = (range.end < end) ? range.right_closed : right_closed;
		}else{
			half->begin = attr();
			half->end = attr();
		}
	}
	/*
	  bool is_invalid()const{
	  return begin.is_invalid() && end.is_invalid();
	  } 
	*/ 
	void get_up_from(const attr& border, attr_range* half)const{
		assert(contain(border));
		assert(!(border == begin));
		half->begin = border;
		half->end = end;
		half->left_closed = true;
		half->right_closed = right_closed;
	}
	void get_down_from(const attr& border, attr_range* half)const{
		assert(contain(border));
		assert(!(border == end));
		half->begin = begin;
		half->end = border;
		half->left_closed = left_closed;
		half->right_closed = false;
	}
	void cut_up_from(const attr& border, attr_range* half){
		assert(contain(border));
		assert(!(border == begin));
		half->begin = border;
		half->end = end;
		half->left_closed = true;
		half->right_closed = right_closed;
		right_closed = false;
		end = border;
	}
	void cut_down_from(const attr& border, attr_range* half){
		assert(contain(border));
		assert(!(border == end));
		half->begin = begin;
		half->end = border;
		half->left_closed = left_closed;
		half->right_closed = false;
		left_closed = true;
		begin = border;
	}
	void cut_uphalf(attr_range* half){
		attr middle = begin.get_middle(end);
		half->begin = middle;
		half->end = end;
		assert(!(middle == begin));
		half->left_closed = true;
		half->right_closed = right_closed;
		right_closed = false;
		end = middle;
	}
	void cut_downhalf(attr_range* half){
		attr middle = begin.get_middle(end);
		half->begin = begin;
		half->end = middle;
		assert(!(middle == end));
		half->left_closed = left_closed;
		half->right_closed = false;
		left_closed = true;
		begin = middle;
	}
	bool contain(const attr& atr)const{
		return (begin < atr && atr < end) ||
			(left_closed && atr == begin) ||
			(right_closed && atr == end);
	}
	
	bool contain(const attr_range& range)const{
		return contain(range.begin) || contain(range.end) ||
			(range.begin < begin && end < range.end);
	}
	bool operator<(const attr_range& rhs)const{
		return begin < rhs.begin;
	}
	const attr& get_begin()const{
		return begin;
	}
	const attr& get_end()const{
		return end;
	}
	std::size_t hash32()const{
		return begin.hash32() + end.hash32();
	}
	
	void dump()const{
		//int type = begin.is_invalid() ? end.get_type() : begin.get_type();
		fprintf(stderr,"%s",left_closed ? "[": "(");
		begin.dump();
		fprintf(stderr," ~ ");
		end.dump();
		fprintf(stderr,"%s",right_closed ? "]": ")");
	}
		
	MSGPACK_DEFINE(begin,end,left_closed,right_closed);
};

class mercury_instance{
	attr_range range;
	std::map<attr_range,address> hubs;
public:
	std::map<attr, uint64_t> kvp;
	mercury_instance():range(){}
	mercury_instance(const attr_range& _range,const std::map<attr_range,address>& _hubs):range(_range),hubs(_hubs){}
	mercury_instance(const mercury_instance& org):range(org.range),hubs(org.hubs){}
	
	const attr_range& get_range()const{
		return range;
	}
	const std::map<attr_range, address>& get_hubs()const{
		return hubs;
	}
		
};


struct range_query_fwd{
	const int id;
	const std::string name;
public:
	range_query_fwd():id(),name(){}
	explicit range_query_fwd(const int _id,const std::string& _name):id(_id),name(_name){}
	explicit range_query_fwd(const range_query_fwd& org):id(org.id),name(org.name){}
	int get_id()const{
		return id;
	}
	const std::string& get_name()const{
		return name;
	}
	bool operator==(const range_query_fwd& rhs)const{
		return id == rhs.id && name == rhs.name;
	}
};
class range_query_hash{
public:
	std::size_t operator()(const range_query_fwd& o)const{
		return o.id ^ hash32(o.name);
	}
};

class mercury_kvp{
public:
	attr id;
	uint64_t data;
	mercury_kvp():id(),data(){}
	mercury_kvp(const std::string _attr, const uint64_t _data):id(std::string(_attr)),data(_data){}
	mercury_kvp(const attr& _attr,const uint64_t _data):id(_attr),data(_data){}
	mercury_kvp(const mercury_kvp& org):id(org.id),data(org.data){}
	const attr& get_attr()const{
		return id;
	}
	const uint64_t& get_data()const{
		return data;
	}
	void dump()const{
		id.dump();
		fprintf(stderr," -> %llu",(unsigned long long)data);
	}
	bool operator<(const mercury_kvp& rhs)const{
		if(data < rhs.data)return true;
		else if(rhs.data < data)return false;
		else if(id < rhs.id) return true;
		else return false;
	}
	bool operator==(const mercury_kvp& rhs)const{
		return data==rhs.data;
	}
	MSGPACK_DEFINE(id, data); // serialize and deserialize ok
};

struct mer_fwd_id{
	std::string name;
	int identifier;
	mer_fwd_id(const std::string& _name, const int _identifier):name(_name),identifier(_identifier){}
	bool operator==(const mer_fwd_id& rhs)const{
		return name == rhs.name && identifier == rhs.identifier;
	}
	bool operator<(const mer_fwd_id& rhs)const{
		if(name < rhs.name) return true;
		else if(rhs.name < name) return false;
		else if(identifier < rhs.identifier) return true;
		else return false;
	}
	mer_fwd_id(const mer_fwd_id& _org):name(_org.name),identifier(_org.identifier){}
private:
	mer_fwd_id& operator=(const mer_fwd_id&);
};

struct mer_get_fwd{
	std::list<mercury_kvp> toSend; // data to send
	int cnt; // set this counter in construction
	const address org; // return address to send
	mer_get_fwd(const int _cnt,const address& _org):cnt(_cnt),org(_org){}
	mer_get_fwd(std::list<mercury_kvp>& _toSend,const int _cnt,const address& _org):toSend(_toSend),cnt(_cnt),org(_org){ }
	//	mer_get_fwd(const mer_get_fwd& _org):cnt(_org.cnt),org(_org.org){}
private:
	mer_get_fwd();
};
struct mer_set_fwd{
	int cnt;
	const address org;
	mer_set_fwd(const int _cnt, const address& _org):cnt(_cnt),org(_org){}
	//mer_set_fwd(const mer_set_fwd& _org):cnt(_org.cnt),org(_org.org){}
private:
	mer_set_fwd();
	mer_set_fwd(const mer_set_fwd&);
};

class mer_fwd_id_hash{
public:
	std::size_t operator()(const mer_fwd_id& o)const{
		return hash32(o.name) ^ hash32(o.identifier);
	}
};
