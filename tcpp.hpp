#ifndef TCPP_HPP_
#define TCPP_HPP_

#include <tcutil.h>
#include <tchdb.h>
#include <tcbdb.h>
#include <tcadb.h>

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>


#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

namespace tokyo_cabinet{

class buff{
public:
    const void* const ptr;
    const int len;
    inline buff(const void* _addr, int _len):ptr(_addr),len(_len){}
    inline buff(const buff& o):ptr(o.ptr),len(o.len){}
    inline buff(const std::string& o):ptr(o.data()),len(o.size()){}
    inline buff(const std::vector<char>& o):ptr(o.data()),len(o.size()){}
private:
    buff();
};
class record{
public:
    void* const ptr;
    const int len;
    record(void* _addr, int _len):ptr(_addr),len(_len){}
    ~record(){
        free(ptr);
    }
private:
    record();
    record(const record&);
};


template<class key, class value>
class map{
    TCMAP* m;
public:

    class iterator{
    public:
        std::pair<const key,value> kvp;
        const int is_null;
        iterator(std::pair<const key,value> _kvp)
            :kvp(_kvp),is_null(false){}
        iterator(const iterator& o)
            :kvp(o.kvp),is_null(false){}
        iterator()
            :kvp(*static_cast<key*>(NULL),*static_cast<value*>(NULL)),is_null(true){}
        std::pair<const key,value>* operator->(){
            return &kvp;
        }
        std::pair<const key,value>& operator*(){
            return kvp;
        }
        bool operator==(const iterator& rhs)const{
            if(is_null && rhs.is_null)return true;
            else if(kvp.first == rhs.kvp.first)return true;
            else return false;
        }
        bool operator!=(const iterator& rhs)const{
            return !(operator==(rhs));
        }
    private:
        iterator& operator++()const;
        iterator& operator--()const;
    };

    inline map():m(tcmapnew()){}
    inline map(const map& org):m(tcmapdup(org.m)){}
    inline map& operator=(const map& rhs){
        tcmapdel(m);
        m = tcmapdup(rhs.m);
        return *this;
    }
    inline void insert(const key& k, const value& v){
        tcmapput(m, &k, sizeof(k), &v, sizeof(v));
    }
    inline void erase(const key& k){
        tcmapout(m, &k, sizeof(k));
    }
    const iterator& end()const{
        static const iterator it;
        return it;
    }
    iterator find(const key& k){
        int length;
        const void* ptr = tcmapget(m, const_cast<long unsigned int*>(&k),
                                   sizeof(k), &length);
        if(ptr != NULL){
            return iterator
                (std::pair<const key,value>
                 (k,*static_cast<value*>(const_cast<void*>(ptr))));
        }
    }
    inline uint64_t size()const{
        return tcmaprnum(m);
    }
    inline uint64_t datasize()const{
        return tcmapmsiz(m);
    }
    ~map(){
        tcmapdel(m);
    }

};

class db_map{
    TCHDB* h;
public:
    inline db_map(const char* filename, int backetsize):h(tchdbnew()){
        tchdbsetmutex(h);
        tchdbtune(h,backetsize,6,14,HDBTLARGE);
        tchdbopen(h,filename,HDBOWRITER | HDBOCREAT | HDBOTRUNC);
    }
    inline void insert(const buff& k, const buff& v){
        bool result = tchdbput(h, k.ptr, k.len, v.ptr, v.len);
        if(!result){
            int ecode = tchdbecode(h);
            fprintf(stderr,"hash db error:%s\n",tchdberrmsg(ecode));
        }
    }
    inline void erase(const buff& k){
        tchdbout(h, k.ptr, k.len);
    }
    inline const record* find(const buff& k){
        int length;
        void* ptr = tchdbget(h, k.ptr, k.len, &length);
        fprintf(stderr,"length: %d\n",length);
        return new record(ptr,length);
    }
    
    ~db_map(){
        tchdbclose(h);
        tchdbdel(h);
    }
};

namespace {
template<typename T>
int cmp(const char* lhs_k, int, const char* rhs_k, int, void*){
    const T& lhs = T(*(T*)lhs_k);
    const T& rhs = T(*(T*)rhs_k);

    if(operator<(lhs,rhs)) return -1;
    else if(operator<(rhs,lhs)) return 1;
    else return 0;
}
template<>
int cmp<std::string>(const char* lhs_k, int, const char* rhs_k, int, void*){
    const std::string& lhs(lhs_k);
    const std::string& rhs(rhs_k);
    
    if(operator<(lhs,rhs)) return -1;
    else if(operator<(rhs,lhs)) return 1;
    else return 0;
}

}
template<typename T>
class tree{
    TCTREE *t;
public:
    inline tree():t(tctreenew2(cmp<T>, NULL)){}
    inline tree(const tree& org):t(tctreedup(org.t)){}
    inline tree& operator=(const tree& rhs){
        tctreedel(t);
        t = tctreedup(rhs.t);
        return *this;
    }
    inline void insert(const buff& k, const buff& v){
        tctreeput(t, k.ptr, k.len, v.ptr, v.len);
    }
    inline void erase(const buff& k){
        tctreeout(t, k.ptr, k.len);
    }
    inline buff find(const buff& k){
        int length;
        const void* ptr = tctreeget(t, k.ptr, k.len, &length);
        return buff(ptr,length);
    }
    inline uint64_t size()const{
        return tctreernum(t);
    }
    inline uint64_t datasize()const{
        return tctreemsiz(t);
    }
    ~tree(){
        tctreedel(t);
    }
};

template<typename T>
class db_tree{
    TCBDB* t;
public:
    struct iterator{
        BDBCUR* cursor;
        iterator():cursor(NULL){}
        iterator(const iterator& org):cursor(org.cursor){}
        iterator& operator=(const iterator& rhs){
            if(cursor){
                tcbdbcurdel(cursor);
            }
            cursor = rhs.cursor;
            return *this;
        }
        iterator& operator ++(){
            int result = tcbdbcurnext(cursor);
            if(!result){
                cursor = iterator(NULL);
            }
            return *this;
        }
        iterator& operator --(){
            int result = tcbdbcurprev(cursor);
            if(!result){
                cursor = iterator(NULL);
            }
            return *this;
        }
        ~iterator(){
            if(cursor){
                tcbdbcurdel(cursor);
            }
        }
        buff first()const{
            int length;
            void* ptr = tcbdbcurkey(cursor, &length);
            return buff(ptr,length);
        }
        buff second()const{
            int length;
            void* ptr = tcbdbcurval(cursor, &length);
            return buff(ptr,length);
        }
    };
    inline db_tree(const char* filename, int backetsize):t(tcbdbnew()){
        tcbdbsetmutex(t);
        tcbdbsetcmpfunc(t, cmp<T>, NULL);
        tcbdbtune(t, 256, 512, backetsize, 9, 10, BDBTLARGE);
        tcbdbopen(t, filename, BDBOWRITER | BDBOCREAT | BDBOTRUNC);
    }
    inline void insert(const buff& k, const buff& v){
        bool result = tcbdbput(t, k.ptr, k.len, v.ptr, v.len);
        if(!result){
            int ecode = tcbdbecode(t);
            fprintf(stderr,"hash db insert error:%s\n",tchdberrmsg(ecode));
        }
    }
    inline void erase(const buff& k){
        bool result = tcbdbout(t, k.ptr, k.len);
        if(!result){
            int ecode = tcbdbecode(t);
            fprintf(stderr,"hash db erase error:%s\n",tchdberrmsg(ecode));
        }
    }
    inline const record* find(const buff& k){
        int length;
        void* ptr = tcbdbget(t, k.ptr, k.len, &length);
        fprintf(stderr,"length: %d\n",length);
        return new record(ptr,length);
    }
    inline iterator find_cursor(const iterator& i,const buff& k){
        bool result = tcbdbcurjump(i.cursor, k.ptr, k.len);
        if(!result){
            i = iterator(NULL);
        }
        return i;
    }
    inline iterator begin()const{
        iterator ans = tcbdbcurnew(t);
        bool result = tcdbcurfirst(ans);
        if(!result){
            ans = NULL;
        }
        return ans;
    }
    inline iterator end()const{
        return iterator(NULL);
    }
    ~db_tree(){
        tcbdbclose(t);
        tcbdbdel(t);
    }
};


template <class key, class value>
class mhash{
    TCMDB* m;






};










} // namespace tokyo_cabinet
#endif
