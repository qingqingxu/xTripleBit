/*
 * Thread.h
 *
 *  Created on: Apr 17, 2014
 *      Author: root
 */

#ifndef THREAD_H_
#define THREAD_H_

#include <iostream>
#include <stdint.h>

class Thread {
public:
	// Create a new thread
	static bool start(void(*start)(void*), void *arg);

	// Wait x ms
	static void sleep(unsigned time);
	// Get the thread id
	static long threadID();
	// Activate the next thread
	static void yield();
	// Get the current time in milliseconds
	static uint64_t getTicks();
};

#endif /* THREAD_H_ */
