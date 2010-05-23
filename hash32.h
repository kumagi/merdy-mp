#ifndef HASH32_H
#define HASH32_H
#include "MurmurHash2.cpp" // MIT Licence


template<typename T>
inline size_t hash32(const T& obj){
	return MurmurHash2(&obj, sizeof(obj), 0);
}
inline size_t hash32(const std::string o){
	return MurmurHash2(o.data(), o.length(), 0);
}

inline size_t hash32(const void* const buff, const int size){
	return MurmurHash2(buff, size, 0);
}

#endif
