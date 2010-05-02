
#include "MurmurHash2_64.cpp" // MIT Licence

#if __x86_64__ || _WIN64
#define _64BIT
#define _ptr long long
#else
#define _32BIT
#define _ptr int
#endif


// hash function
#ifdef _64BIT
template<typename T> 
inline long long hash_value(const T& obj,unsigned int seed=0){
	return MurmurHash64A(&obj, sizeof(obj), seed);
}
inline long long hash_value(const std::string& data, unsigned int seed=0){
	return MurmurHash64A(data.c_str(),data.length(), seed);
}
#else
template<typename T> 
inline long long hash_value(const T& obj,unsigned int seed=0){
	return MurmurHash64B(&obj, sizeof(obj), seed);
}
inline long long hash_value(const std::string& data, unsigned int seed=0){
	return MurmurHash64B(data.c_str(),data.length(), seed);
}
#endif
