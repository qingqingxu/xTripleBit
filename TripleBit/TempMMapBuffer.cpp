/*
 * TempMMapBuffer.cpp
 *
 *  Created on: 2014-1-14
 *      Author: root
 */

#include "TempMMapBuffer.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <stdio.h>
#include "MemoryBuffer.h"

TempMMapBuffer *TempMMapBuffer::instance = NULL;

TempMMapBuffer::TempMMapBuffer(const char *_filename, size_t initSize) :
		filename(_filename) {
	fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
	if (fd < 0) {
		perror(_filename);
		MessageEngine::showMessage("Create tempMap file error", MessageEngine::ERROR);
	}

	size = lseek(fd, 0, SEEK_END);
	if (size < initSize) {
		size = initSize;
		if (ftruncate(fd, initSize) != 0) {
			perror(_filename);
			MessageEngine::showMessage("ftruncate file error", MessageEngine::ERROR);
		}
	}
	if (lseek(fd, 0, SEEK_SET) != 0) {
		perror(_filename);
		MessageEngine::showMessage("lseek file error", MessageEngine::ERROR);
	}

	mmapAddr = (uchar volatile*) mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (mmapAddr == (uchar volatile *) MAP_FAILED) {
		perror(_filename);
		cout << "size: " << size << endl;
		MessageEngine::showMessage("map file to memory error", MessageEngine::ERROR);
	}

	usedPage = 0;
	pthread_mutex_init(&mutex, NULL);
}

TempMMapBuffer::~TempMMapBuffer() {
	flush();
	munmap((uchar*) mmapAddr, size);
	close(fd);
	pthread_mutex_destroy(&mutex);
}

Status TempMMapBuffer::flush() {
	if (msync((uchar*) mmapAddr, size, MS_SYNC) == 0) {
		return OK;
	}
	return ERROR;
}

uchar *TempMMapBuffer::resize(size_t incrementSize) {
	size_t newSize = size + incrementSize;

	uchar *newAddr = NULL;
	if (munmap((uchar*) mmapAddr, size) != 0) {
		MessageEngine::showMessage("resize-munmap error!", MessageEngine::ERROR);
		return NULL;
	}
	if (ftruncate(fd, newSize) != 0) {
		MessageEngine::showMessage("resize-ftruncate file error!", MessageEngine::ERROR);
		return NULL;
	}
	if ((newAddr = (uchar*) mmap(NULL, newSize, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == (uchar*) MAP_FAILED) {
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return NULL;
	}
	mmapAddr = (uchar volatile*) newAddr;
	::memset((uchar*) mmapAddr + size, 0, incrementSize);

	size = newSize;
	return (uchar*) mmapAddr;
}

void TempMMapBuffer::discard() {
	munmap((uchar*) mmapAddr, size);
	close(fd);
	unlink(filename.c_str());
}

uchar *TempMMapBuffer::getBuffer() {
	return (uchar*) mmapAddr;
}

uchar *TempMMapBuffer::getBuffer(int pos) {
	return (uchar*) mmapAddr + pos;
}

Status TempMMapBuffer::resize(size_t newSize, bool clear) {
	uchar *newAddr = NULL;
	if (munmap((uchar*) mmapAddr, size) != 0 || ftruncate(fd, newSize) != 0 || (newAddr = (uchar*) mmap(NULL, newSize, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == (uchar*) MAP_FAILED) {
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return ERROR;
	}

	mmapAddr = (uchar volatile*) newAddr;

	::memset((uchar*) mmapAddr + size, 0, newSize - size);
	size = newSize;
	return OK;
}

void TempMMapBuffer::memset(char value) {
	::memset((uchar*) mmapAddr, value, size);
}

void TempMMapBuffer::create(const char *filename, size_t initSize = TEMPMMAPBUFFER_INIT_PAGE * MemoryBuffer::pagesize) {
//	initSize *= 10;
	instance = new TempMMapBuffer(filename, initSize);
}

TempMMapBuffer &TempMMapBuffer::getInstance() {
	if (instance == NULL) {
		perror("instance must not be NULL");
	}
	return *instance;
}

void TempMMapBuffer::deleteInstance() {
	if (instance != NULL) {
		instance->discard();
		instance = NULL;
	}
}

uchar *TempMMapBuffer::getPage(size_t &pageNo) {
	pthread_mutex_lock(&mutex);
	uchar *rt;
	if (usedPage * MemoryBuffer::pagesize >= size) {
		resize(size);
	}
	pageNo = usedPage;
	rt = getAddress() + usedPage * MemoryBuffer::pagesize;
	usedPage++;
	pthread_mutex_unlock(&mutex);
	return rt;
}
