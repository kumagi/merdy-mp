#include <boost/noncopyable.hpp>

class mutex: public boost::noncopyable{
    pthread_mutex_t mut;
    friend class scoped_lock;

public:
    class scoped_lock: public boost::noncopyable{
        mutex* mut_;
        bool locked;
    public:
        scoped_lock(mutex& target):mut_(&target),locked(true){
            mut_->lock();
        }
        ~scoped_lock(){
            unlock();
        }
        void unlock(){
            if(locked){
                mut_->unlock();
                locked = false;
            }
        }
        void lock(){
            if(!locked){
                mut_->lock();
                locked = true;
            }
        }
    private:
        scoped_lock();
    };
    mutex(){
        pthread_mutex_init(&mut,0);
    }
    ~mutex(){
        pthread_mutex_destroy(&mut);
    }
    void lock(){
        pthread_mutex_lock(&mut);
    }
    void unlock(){
        pthread_mutex_unlock(&mut);
    }
};
