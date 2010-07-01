#include <stdint.h>
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
#define MurmurHash64 MurmurHash64A
#else
#define MurmurHash64 MurmurHash64B
#endif
template<typename T>
inline uint64_t hash64(const T& obj,unsigned int seed=0){
	return MurmurHash64(&obj, sizeof(obj), seed);
}
inline uint64_t hash64(const void* const ptr, const int len,unsigned int seed=0){
	return MurmurHash64(ptr, len, seed);
}
inline uint64_t hash64(const std::string& data, unsigned int seed=0){
	return MurmurHash64(data.c_str(),data.length(), seed);
}
 
