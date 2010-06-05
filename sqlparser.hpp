namespace sqlparser{

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

inline bool chk_number(const char* str, int length){
	//	DEBUG_OUT("is_number(%s,%d)",str,length);
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
	enum type{
		query,
		string,
		invalid,
	};
	enum type query_or_string;
	enum sql::sql query_;
	std::string string_;
	explicit segment(const sql::sql _query):query_or_string(type::query),query_(_query),string_(){};
	explicit segment(const std::string& _str):query_or_string(type::string),query_(sql::invalid),string_(_str){};
	segment():query_or_string(type::invalid),query_(sql::invalid),string_(){}
	segment(const segment& org):query_or_string(org.query_or_string),query_(org.query_),string_(org.string_){};
	// setter
	segment& operator=(const segment& rhs){
		query_or_string = rhs.query_or_string;
		query_ = rhs.query_;
		string_ = rhs.string_;
		return *this;
	}
	bool operator==(const sql::sql rhs)const{
		return query_or_string == type::query && query_ == rhs;
	}
	bool operator==(const std::string& rhs)const{
		return query_or_string == type::string && string_ == rhs;
	}
	void set_query(const sql::sql _query){
		query_or_string = type::query;
		query_ = _query;
		string_.clear();
	}
	void set_string(const std::string& _str){
		query_or_string = type::string;
		query_ = sql::invalid;
		string_ = _str;
	}
	// getter
	enum sql::sql get_query(void)const{
		if(query_or_string != type::query){
			assert(!"dont get other type: query");
		}
		return query_;
	}
	const std::string& get_string(void)const{
		if(query_or_string != type::string){
			assert(!"dont get other type: data");
		}
		return string_;
	}
	bool is_query()const{
		return query_or_string == type::query;
	}
	bool is_string()const{
		return query_or_string == type::string;
	}
	bool is_number()const{
		return query_or_string == type::string && chk_number(string_.data(),string_.length());
	}
	int get_number()const{
		assert(is_number());
		return atoi(string_.c_str());
	}
	bool is_invalid()const{
		return query_or_string == type::invalid;
	}
	void dump()const{
		if(query_or_string == type::query){
			fprintf(stderr,"%d ",query_);
		}else if(query_or_string == type::string){
			fprintf(stderr,"[%s]",string_.c_str());
		}else if(query_or_string == type::invalid){
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
				DEBUG_OUT("str[%s]:",token.c_str());
				parsed.push_back(segment(std::string(token)));
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
