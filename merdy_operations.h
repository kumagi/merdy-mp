
enum flag{
	TYPE_OTHER = -1,
	TYPE_INT = 0,
	TYPE_STR = 1,
};

namespace OP{
enum merdy_operations{
	// master
	SEND_DY_LIST, // 
	SEND_HASHES, // 
	SEND_MER_LIST, // 
	CREATE_SCHEMA, // 
	DELETE_SCHEMA, // 
	ADD_ME_DY, // 
	ADD_ME_MER, // 
	TELLME_HASHES,
		
	// dynamo
	//setting
	UPDATE_HASHES, // 
	OK_ADD_ME_DY, // 
	//writing
	SET_DY, // search cordinate
	OK_SET_DY, // 
	PUT_DY, // store data without everything
	OK_PUT_DY, // 
	SET_COORDINATE, // 
	//reading
	GET_DY,
	SEND_DY,
	FOUND_DY,
	NOTFOUND_DY,
	DEL_DY,
	
	// merdy
	OK_ADD_ME_MER, // int,std::set<address
	UPDATE_MER_HUB,
	ASSIGN_ATTR,
	SET_ATTR, // int, std::string, std::list<mercury_kvp>, address
	OK_SET_ATTR,
	GET_ATTR, // int, std::string, int, std::list<attr>, address
	GET_RANGE,
	TELLME_RANGE,
	TELLME_ASSIGN,
	OK_TELLME_RANGE,
	NG_TELLME_RANGE,
	GIVEME_RANGE,
	ASSIGN_RANGE,
	OK_ASSIGN_RANGE,
	NG_ASSIGN_RANGE,
	DEL_RANGE,
	
	
	
	// mercury -> proxy
	OK_GET_RANGE, // int, std::string, std::list<mercury_kvp
	OK_GET_ATTR, // int, std::string, std::list<mercury_kvp>
	NG_GET_ATTR, // int, std:;string
	
	
	// master -> proxy
	OK_CREATE_SCHEMA,
	ASSIGNMENT, // int, std::string, std::list<address> 
	NO_ASSIGNMENT, // int, std::string
	
	
	// user -> proxy
	SET_KVP,
	DO_SQL,
};
}

namespace DATA{
enum type{
	INT,
	STRING,
};
}
class value_vclock;
class address;
class attr;
class attr_range;
class mercury_kvp;

namespace MERDY{
// * -> master
typedef msgpack::type::tuple<int,address> add_me_dy;
typedef msgpack::type::tuple<int,address> add_me_mer;
typedef msgpack::type::tuple<int,std::string> ok_create_schema;
typedef msgpack::type::tuple<int,std::string,address> tellme_assign;
typedef msgpack::type::tuple<int,std::string,address> tellme_range;
typedef msgpack::type::tuple<int,std::string> ok_assign_range;
typedef msgpack::type::tuple<int,std::string> ng_assign_range;
typedef msgpack::type::tuple<int,address> tellme_hashes;

// master -> dynamo
typedef msgpack::type::tuple<int,std::map<uint64_t,address> > update_hashes;

// proxy/dynamo -> dynamo
typedef msgpack::type::tuple<int,uint64_t,address> get_dy;
typedef msgpack::type::tuple<int,std::set<address> > update_mer_hub;
typedef msgpack::type::tuple<int,uint64_t,std::list<attr>,address> set_dy;
typedef msgpack::type::tuple<int,uint64_t,std::list<attr>,address> set_coordinate;
typedef msgpack::type::tuple<int,uint64_t,value_vclock, address> put_dy;
typedef msgpack::type::tuple<int,uint64_t,address> send_dy;

// proxy/dynamo -> dynamo/proxy
typedef msgpack::type::tuple<int,uint64_t> ok_put_dy;
typedef msgpack::type::tuple<int,uint64_t> ok_set_dy;
typedef msgpack::type::tuple<int,uint64_t,value_vclock,address> found_dy;
typedef msgpack::type::tuple<int,uint64_t,address> notfound_dy;

// master -> mercury
typedef msgpack::type::tuple<int,std::string, int, address> create_schema;
typedef msgpack::type::tuple<int,std::set<address> > ok_add_me_mer;

// proxy -> mercury
typedef msgpack::type::tuple<int,std::string,int,attr_range,address> get_range;
typedef msgpack::type::tuple<int,std::string,int,std::list<mercury_kvp>, address > set_attr;
typedef msgpack::type::tuple<int,std::string,int,std::list<attr>,address> get_attr;

// mercury/proxy -> mercury/proxy
typedef msgpack::type::tuple<int,std::string,int,std::list<mercury_kvp> > ok_get_range;
typedef msgpack::type::tuple<int,std::string,int,std::list<mercury_kvp> > ok_get_attr;
typedef msgpack::type::tuple<int,std::string,int> ok_set_attr;
typedef msgpack::type::tuple<int,std::string,attr_range> ok_tellme_range;
typedef msgpack::type::tuple<int,std::string> ng_tellme_range;
typedef msgpack::type::tuple<int,std::string,attr_range, std::map<attr_range, address>, address > assign_range;
typedef msgpack::type::tuple<int,std::string,std::map<attr_range,address> > assignment;
typedef msgpack::type::tuple<int,std::string> no_assignment;
typedef msgpack::type::tuple<int,std::string,int> ng_get_attr;
}
