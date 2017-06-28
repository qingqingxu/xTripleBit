/*
 * TempBuffer.cpp
 *
 *  Created on: 2013-7-10
 *      Author: root
 */

#include "TempBuffer.h"
#include "MemoryBuffer.h"
#include <math.h>
#include <pthread.h>

TempBuffer::TempBuffer() {
	// TODO Auto-generated constructor stub
	buffer = (ChunkTask::ChunkTriple*)malloc(TEMPBUFFER_INIT_PAGE_COUNT * getpagesize());
	usedSize = 0;
	totalSize = TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() / sizeof(ChunkTask::ChunkTriple);
	pos = 0;
}

TempBuffer::~TempBuffer() {
	// TODO Auto-generated destructor stub
	if(buffer != NULL)
		free(buffer);
	buffer = NULL;
}

Status TempBuffer::insertTriple(ID subjectID, double object, char objType)
{
	buffer[pos].subjectID = subjectID;
	buffer[pos].object = object;
	buffer[pos].objType = objType;
	pos++;
	usedSize++;
	return OK;
}

Status TempBuffer::clear()
{
	pos = 0;
	usedSize = 0;
	return OK;
}

ChunkTask::ChunkTriple& TempBuffer::operator[](const size_t index){
	if(index >= usedSize){
		return buffer[0];
	}
	return buffer[index];
}

void TempBuffer::Print()
{
	for(int i = 0; i < usedSize; ++i)
	{
		cout << "subjectID:" << buffer[i].subjectID;
		cout << " object:" << buffer[i].object << " ";
		cout << " objType:" << buffer[i].objType << " ";
	}
	cout << endl;
}

int cmpByS(const void *lhs, const void *rhs)
{
	ChunkTask::ChunkTriple* lTriple = (ChunkTask::ChunkTriple*) lhs;
	ChunkTask::ChunkTriple* rTriple = (ChunkTask::ChunkTriple*) rhs;
	if(lTriple->subjectID != rTriple->subjectID){
		return lTriple->subjectID - rTriple->subjectID;
	}else {
		return lTriple->object - rTriple->object;
	}
}

int cmpByO(const void *lhs, const void *rhs)
{
	ChunkTask::ChunkTriple* lTriple = (ChunkTask::ChunkTriple*) lhs;
	ChunkTask::ChunkTriple* rTriple = (ChunkTask::ChunkTriple*) rhs;
	if(lTriple->object != rTriple->object){
		return lTriple->object - rTriple->object;
	}else {
		return lTriple->subjectID - rTriple->subjectID;
	}
}

Status TempBuffer::sort(bool soType)
{
	if(soType == ORDERBYS){
		qsort(buffer, usedSize, sizeof(ChunkTask::ChunkTriple), cmpByS);
	}else if(soType == ORDERBYO){
		qsort(buffer, usedSize, sizeof(ChunkTask::ChunkTriple), cmpByO);
	}

	return OK;
}

bool TempBuffer::isEquals(ChunkTask::ChunkTriple* lTriple, ChunkTask::ChunkTriple* rTriple){
	if(lTriple->subjectID == rTriple->subjectID && lTriple->object == rTriple->object){
		return true;
	}
	return false;
}

void TempBuffer::uniqe()
{
	if(usedSize <= 1) return;
	ChunkTask::ChunkTriple *lastPtr, *currentPtr, *endPtr;
	lastPtr = currentPtr = buffer;
	endPtr = getEnd();
	currentPtr++;
	while(currentPtr < endPtr)
	{
		if(isEquals(lastPtr, currentPtr)){
			currentPtr++;
		}else
		{
			lastPtr++;
			*lastPtr = *currentPtr;
			currentPtr++;
		}
	}
	usedSize = lastPtr - buffer;
}

