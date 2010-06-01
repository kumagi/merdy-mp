namespace sqlparser{
namespace type{
enum query_data{
	query,
	data,
	invalid,
};
}
namespace sql{
enum sql{
	create,
	table,
	insert,
	into,
	select,
	from,
	where,
	data,
	data_char,
	data_int,
	left_brac,
	right_brac,
	values,
	comma,
	op_eq,
	op_gt,
	op_lt,
	op_ne,
	op_and,
	op_or,
	op_star,
	semicolon,
	raw_int,
	raw_char,
	invalid,
};
}

inline bool is_number(const char* str, int length){
	DEBUG_OUT("is_number(%s,%d)",str,length);
	while(length > 0){
		if(*str < '0' || '9' < *str){
			return false;
		}
		++str;
		--length;
	}
	return true;
}
struct segment{
	// query or data
	type::query_data query_or_data;
	int query;
	attr data;
	explicit segment(const int _query):query_or_data(type::query),query(_query),data(){};
	explicit segment(const attr& _attr):query_or_data(type::data),query(sql::invalid),data(_attr){};
	segment():query_or_data(type::invalid),query(sql::invalid),data(){}
	segment(const segment& org):query_or_data(org.query_or_data),query(org.query),data(org.data){};
	// setter
	segment& operator=(const segment& rhs){
		query_or_data = rhs.query_or_data;
		query = rhs.query;
		data = rhs.data;
		return *this;
	}
	bool operator==(const int rhs)const{
		return query_or_data == type::query && query == rhs;
	}
	bool operator==(const attr& rhs)const{
		return query_or_data == type::data && data == rhs;
	}
	void set_query(const int _query){
		query_or_data = type::query;
		query = _query;
		data = attr();
	}
	void set_data(const attr& _data){
		query_or_data = type::data;
		query = 0;
		data = _data;
	}
	// getter
	int get_query(void)const{
		if(query_or_data != type::query){
			assert(!"dont get other type: query");
		}
		return query;
	}
	const attr& get_data(void)const{
		if(query_or_data != type::data){
			assert(!"dont get other type: data");
		}
		return data;
	}
	bool is_query()const{
		return query_or_data == type::query;
	}
	bool is_data()const{
		return query_or_data == type::data;
	}
	bool is_invalid()const{
		return query_or_data == type::invalid;
	}
	void dump()const{
		if(query_or_data == type::query){
			fprintf(stderr,"q%d ",query);
		}else if(query_or_data == type::data){
			fprintf(stderr,"a");
			data.dump();
		}else if(query_or_data == type::invalid){
			fprintf(stderr,"# ");
		}
	}
};


class query{
	const std::string data;
	std::list<segment> parsed;
	std::list<segment>::iterator parsed_it;
public:
	query(const std::string& _data):data(_data){
		parse();
		parsed_it = parsed.begin();
	}
	const segment& get()const{
		return *parsed_it;
	}
	bool next(){
		++parsed_it;
		if(parsed_it == parsed.end()){return false;}
		else {return true;}
	}
private:
	class tokenize{
		const std::string& data;
		unsigned int offset;
	public:
		tokenize(const std::string _data):data(_data),offset(0){};
		const std::string next(void){
			const char* ptr = &data.c_str()[offset];
			if(ptr == '\0') return std::string("");
			while(is_brank(*ptr)){++ptr;++offset;}
			const char* origin = ptr;
			while(1){
				if(is_brank(*ptr) || is_code(*ptr) || *ptr == '\0'){
					break;
				}
				++ptr;
			}
			
			int diff= ptr - origin;
			if(diff == 0){
				if(*ptr == '\0') return std::string(" ");
				else {
					offset++;
					char bytes[2];
					bytes[0] = *ptr;
					bytes[1] = '\0';
					return std::string(bytes);
				}
			}else{
				offset += diff;
				if(is_brank(*ptr)){offset++;}
				return std::string(origin,diff);
			}
		}
	private:
		bool is_brank(const char& d)const{
			return d == ' ' || d == '\t' || d == '\n';
		}
		bool is_code(const char& d)const{
			return d == ',' || d == '(' || d == ')' || d == ',' || d == '=' || d == '<' || d == '>' || d == '!' || d == '&' || d == '|' || d == '*' || d == ';'; 
		}
	};
	void parse(){
		tokenize target(data);
		while(1){
			const std::string& token(target.next());
			if(token == std::string("CREATE") || token == std::string("create")){
				DEBUG_OUT("create:");
				parsed.push_back(segment(sql::create));
			}else if(token == std::string("TABLE") || token == std::string("table")){
				DEBUG_OUT("table:");
				parsed.push_back(segment(sql::table));
			}else if(token == std::string("VALUES") || token == std::string("values")){
				DEBUG_OUT("values:");
				parsed.push_back(segment(sql::values));
			}else if(token == std::string("INSERT") || token == std::string("insert")){
				DEBUG_OUT("insert:");
				parsed.push_back(segment(sql::insert));
			}else if(token == std::string("INTO") || token == std::string("into")){
				DEBUG_OUT("into:");
				parsed.push_back(segment(sql::into));
			}else if(token == std::string("INT") || token == std::string("int")){
				DEBUG_OUT("INT:");
				parsed.push_back(segment(sql::data_int));
			}else if(token == std::string("CHAR") || token == std::string("char")){
				DEBUG_OUT("CHAR:");
				parsed.push_back(segment(sql::data_char));
			}else if(token == std::string("SELECT") || token == std::string("select")){
				DEBUG_OUT("SELECT:");
				parsed.push_back(segment(sql::select));
			}else if(token == std::string("WHERE") || token == std::string("where")){
				DEBUG_OUT("where:");
				parsed.push_back(segment(sql::where));
			}else if(token == std::string("FROM") || token == std::string("from")){
				DEBUG_OUT("from:");
				parsed.push_back(segment(sql::from));
			}else if(token == std::string("&") || token == std::string("AND") || token == std::string("and")){
				DEBUG_OUT("&:");
				parsed.push_back(segment(sql::op_and));
			}else if(token == std::string("|") ||token == std::string("OR") || token == std::string("or")){
				DEBUG_OUT("|:");
				parsed.push_back(segment(sql::op_or));
			}else if(token == std::string("(")){
				DEBUG_OUT("(:");
				parsed.push_back(segment(sql::left_brac));
			}else if(token == std::string(")")){
				DEBUG_OUT("):");
				parsed.push_back(segment(sql::right_brac));
			}else if(token == std::string(",")){
				DEBUG_OUT(",:");
				parsed.push_back(segment(sql::comma));
			}else if(token == std::string("=")){
				DEBUG_OUT("=:");
				parsed.push_back(segment(sql::op_eq));
			}else if(token == std::string(">")){
				DEBUG_OUT(">:");
				parsed.push_back(segment(sql::op_gt));
			}else if(token == std::string("<")){
				DEBUG_OUT("<:");
				parsed.push_back(segment(sql::op_lt));
			}else if(token == std::string("*")){
				DEBUG_OUT("*:");
				parsed.push_back(segment(sql::op_star));
			}else if(token == std::string(";")){
				DEBUG_OUT(";:");
				parsed.push_back(segment(sql::semicolon));
			}else if(token == std::string(" ")){
				DEBUG_OUT("ok\n");
				break;
			}else{
				if(!is_number(token.data(),token.length())){ // string
					DEBUG_OUT("str[%s]:",token.c_str());
					parsed.push_back(segment(attr(std::string(token))));
				}else{ // int 
					DEBUG_OUT("int[%s]:",token.c_str());
					parsed.push_back(segment(attr(atoi(token.c_str()))));
				}
			}
		}
		
		// parse ok
		std::list<segment>::iterator it = parsed.begin();
	}
private:// forbid methods
	query();
	query(query&);
	query& operator=(query&);
};

}// sqlparser
