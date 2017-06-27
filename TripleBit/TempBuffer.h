/*
 * TempBuffer.h
 *
 *  Created on: 2013-7-10
 *      Author: root
 */

#ifndef TEMPBUFFER_H_
#define TEMPBUFFER_H_

#include "TripleBit.h"
#include "ThreadPool.h"

class TempBuffer {
public:
	SOCouple buffer[TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() / sizeof(SOCouple)];
	int pos; //buffer's index

	size_t usedSize;
	size_t totalSize;

public:
	Status insertTriple(ID subjectID, double object, char objType = STRING);
	void Print();
	Status sort(bool soType);
	void uniqe();
	Status clear();
	SOCouple& operator[](const size_t index);
	bool isEquals(SOCouple* lTriple, SOCouple* rTriple);
	bool isFull() { return usedSize > totalSize-2; }
	bool isEmpty() { return usedSize == 0; }
	size_t getSize() const{
		return usedSize;
	}
	SOCouple* getBuffer() const{
		return buffer;
	}

	SOCouple* getEnd(){
		return getBuffer() + usedSize;
	}

	TempBuffer();
	~TempBuffer();

};

#endif /* TEMPBUFFER_H_ */
