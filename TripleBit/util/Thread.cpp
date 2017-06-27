/*
 * Thread.cpp
 *
 *  Created on: Apr 17, 2014
 *      Author: root
 */

#include "Thread.h"
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>

using namespace std;

bool Thread::start(void (*starter)(void*), void *arg){
	pthread_t tid = 0;
	int result = pthread_create(&tid, NULL, reinterpret_cast<void*(*)(void*)>(starter), arg);
	if(result != 0)
		return false;
	pthread_detach(tid);

	return true;
}

void Thread::sleep(unsigned time){
	// Wait x ms
	if(!time){
		sched_yield();
	}else{
		struct timespec a, b;
		a.tv_sec = time/1000; a.tv_nsec = (time%1000)*1000000;
		nanosleep(&a, &b);
	}
}


long Thread::threadID(){
	// Threadid
	union { pthread_t a; long b; } c;
	c.b =  0;
	c.a = pthread_self();
	return c.b;
}

void Thread::yield(){
	sched_yield();
}

uint64_t Thread::getTicks(){
	timeval t;
	gettimeofday(&t, 0);
	return static_cast<uint64_t>(t.tv_sec)*1000 + (t.tv_usec/1000);
}
