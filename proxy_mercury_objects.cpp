struct mercury_request_fowards{
	const std::string name;
	const int identifier;
	mercury_request_fowards(const std::string& _name, const int _identifier)
		:name(_name),identifier(_identifier){}
	bool operator==(const mercury_request_fowards& rhs)const{
		return name == rhs.name && identifier == rhs.identifier;
	}
};
class mercury_request_fowards_hash{
public:
	std::size_t operator()(const mercury_request_fowards& o)const{
		return hash32(o.name) ^ o.identifier;
	}
};
