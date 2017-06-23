/*
 * TempMMapBuffer.h
 *
 *  Created on: 2014-1-14
 *      Author: root
 */

#ifndef TEMPMMAPBUFFER_H_
#define TEMPMMAPBUFFER_H_

#include "TripleBit.h"

class TempMMapBuffer
{
private:
	int fd;
	uchar volatile *mmapAddr;
	string filename;
	size_t size;

	size_t usedPage;
	pthread_mutex_t mutex;

	static TempMMapBuffer *instance;

private:
	TempMMapBuffer(const char *filename, size_t initSize);
	~TempMMapBuffer();
	uchar *resize(size_t incrementSize);
	Status resize(size_t newSize, bool clear);
	void memset(char value);

public:
	uchar *getBuffer();
	uchar *getBuffer(int pos);
	void discard();
	Status flush();
	size_t getSize(){ return size; }
	size_t getLength() { return size; }
	uchar *getAddress() const { return (uchar*)mmapAddr; }
	uchar *getPage(size_t &pageNo);
	size_t getUsedPage(){ return usedPage; }

public:
	static void create(const char *filename, size_t initSize);
	static TempMMapBuffer &getInstance();
	static void deleteInstance();
};

#endif /* TEMPMMAPBUFFER_H_ */
