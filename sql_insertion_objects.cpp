
class sql_answer{
public:
	const int org;
	std::string buff;
	sql_answer(const int& _org, const std::string& _buff)
		:org(_org),buff(_buff){}
	sql_answer(const sql_answer& _org)
		:org(_org.org),buff(_org.buff){}

	~sql_answer(){
		write(org,buff.data(),buff.length());
	}
private:
	sql_answer();
};

class sql_insert_workingset{
public:
	std::unordered_map<std::string,attr> tuple;
	const uint64_t hashed_tuple;
	const int org_fd;
	mp::wavy::loop* lo_;
	sql_insert_workingset(const uint64_t& _hashed_tuple, const int _org_fd, mp::wavy::loop* _lo_)
		:hashed_tuple(_hashed_tuple),org_fd(_org_fd),lo_(_lo_){}
	~sql_insert_workingset(){
	}
	std::string execute(){
		return std::string("");
	}
private:
	sql_insert_workingset(void);
};
