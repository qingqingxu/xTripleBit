/*
 * Event.h
 *
 *  Created on: Apr 17, 2014
 *      Author: root
 */

#ifndef EVENT_H_
#define EVENT_H_

#include "../TripleBit.h"
#include <pthread.h>

class Mutex;

class Event {
private:
	pthread_cond_t condVar;

	Event(const Event&);
	void operator=(const Event&);

public:
	/// Constructor
	Event();
	/// Destructor
	~Event();

	/// Wait for event. The mutex be locked
	void wait(Mutex &mutex);
	/// Wait up to a certain time. The mutex must be locked
	bool timedWait(Mutex &mutex, unsigned timeoutMilli);
	/// Notify at lease one waiting thread
	void notify(Mutex &mutex);
	/// Notify all waiting threads
	void notifyAll(Mutex &mutex);

};

#endif /* EVENT_H_ */
