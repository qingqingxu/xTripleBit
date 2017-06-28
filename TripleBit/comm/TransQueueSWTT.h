/*
 * TransQueueSWTT.h
 *
 *  Created on: 2014-2-11
 *      Author: root
 */

#ifndef TRANSQUEUESWTT_H_
#define TRANSQUEUESWTT_H_

#include "../TripleBit.h"
#include "Tools.h"

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

using namespace std;
using namespace boost;

class TransQueueSWTT{
private:
	int tail, head, length;
	uint64_t timeBase;
	Transaction *transQueue[MAXTRANSNUM+1];

	mutex mu;
	condition_variable_any cond_enqueue;
	condition_variable_any cond_dequeue;

public:
	bool Queue_Empty() { return head == tail; }
	bool Queue_Full() { return (tail % length + 1) == head; }

	TransQueueSWTT():tail(1), head(1), length(MAXTRANSNUM), timeBase(0){}

	void setTimeBase(uint64_t time) { timeBase = time; }
	uint64_t getTimeBase() { return timeBase; }

	void EnQueue(Transaction *trans){
		{
			mutex::scoped_lock lock(mu);
			while(Queue_Full()){
				cond_enqueue.wait(mu);
			}
			transQueue[tail] = trans;
			tail = (tail == length)? 1:(tail+1);
		}
		cond_dequeue.notify_one();
	}

	void EnQueue(string &queryStr){
		Transaction *trans = new Transaction(queryStr);
		{
			mutex::scoped_lock lock(mu);
			while(Queue_Full()){
				cond_enqueue.wait(mu);
			}
			transQueue[tail] = trans;
			tail = (tail == length)? 1:(tail+1);
		}
		cond_dequeue.notify_one();
	}

	Transaction* DeQueue(){
		mutex::scoped_lock lock(mu);
		while(Queue_Empty()){
			cond_dequeue.wait(mu);
		}
		Transaction *trans = transQueue[head];
		head = (head == length)? 1:(head+1);
		cond_enqueue.notify_one();
		return trans;
	}
};

#endif /* TRANSQUEUESWTT_H_ */
