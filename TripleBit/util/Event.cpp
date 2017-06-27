/*
 * Event.cpp
 *
 *  Created on: Apr 17, 2014
 *      Author: root
 */

#include "Event.h"
#include "Mutex.h"
#include <errno.h>
#include <sys/time.h>

Event::Event(){
	// Constructor
	pthread_cond_init(&condVar, 0);
}

Event::~Event(){
	// Destructor
	pthread_cond_destroy(&condVar);
}

void Event::wait(Mutex &mutex){
	// Wait for event. The mutex must be locked.
	pthread_cond_wait(&condVar, &mutex.mutex);
}

bool Event::timedWait(Mutex &mutex, unsigned timeoutMilli){
	// Wait up to a certain time. The mutex must be locked
	struct timeval now; gettimeofday(&now, 0);
	uint64_t nowT = (static_cast<uint64_t>(now.tv_sec)*1000) + (now.tv_usec/1000);
	uint64_t future = nowT+timeoutMilli;
	struct timespec abstime;
	abstime.tv_sec = future/1000; abstime.tv_nsec = (future%1000)*1000000;
	return pthread_cond_timedwait(&condVar, &(mutex.mutex), &abstime) != ETIMEDOUT;
}

void Event::notify(Mutex &/*mutex*/){
	// Notify at least one waiting thread
	pthread_cond_signal(&condVar);
}

void Event::notifyAll(Mutex &/*mutex*/){
	// Notify all waiting threads
	pthread_cond_broadcast(&condVar);
}



