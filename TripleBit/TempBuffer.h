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
	ChunkTask::ChunkTriple buffer[TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() / sizeof(ChunkTask::ChunkTriple)];
	int pos; //buffer's index

	size_t usedSize;
	size_t totalSize;

public:
	Status insertTriple(ID subjectID, double object, char objType = STRING);
	void Print();
	Status sort(bool soType);
	void uniqe();
	Status clear();
	ChunkTask::ChunkTriple& operator[](const size_t index);
	bool isEquals(ChunkTask::ChunkTriple* lTriple, ChunkTask::ChunkTriple* rTriple);
	bool isFull() { return usedSize > totalSize-2; }
	bool isEmpty() { return usedSize == 0; }
	size_t getSize() const{
		return usedSize;
	}
	ChunkTask::ChunkTriple* getBuffer() const{
		return buffer;
	}

	ChunkTask::ChunkTriple* getEnd(){
		return getBuffer() + usedSize;
	}

	TempBuffer();
	~TempBuffer();

};

#endif /* TEMPBUFFER_H_ */
