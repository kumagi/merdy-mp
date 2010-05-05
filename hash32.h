
#include "MurmurHash2A.cpp" // MIT Licence


template<typename T>
inline size_t hash_value(const T& obj,unsigned int seed=0){
	return MurmurHash2A(&obj, sizeof(obj), seed);
}
