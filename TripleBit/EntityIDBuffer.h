/*
 * EntityIDBuffer.h
 *
 *  Created on: May 21, 2010
 *      Author: liupu
 */

#ifndef ENTITYIDBUFFER_H_
#define ENTITYIDBUFFER_H_

class MemoryBuffer;
class EntityIDBuffer;

#include "TripleBit.h"
#include "ThreadPool.h"

class SortTask {
public:
	SortTask() {
	}
	virtual ~SortTask() {
	}
	static int Run(ID* p, size_t length, int sortKey, int IDCount);
	static int qcompareInt(const void* a, const void* b);
	static int qcompareLongByFirst32(const void* a, const void* b);
	static int qcompareLongBySecond32(const void* a, const void* b);

	static bool compareInt(ID a, ID b);
	static bool compareLongByFirst32(int64_t a, int64_t b);
	static bool compareLongBySecond32(int64_t a, int64_t b);
};

class MidResultBuffer {
public:
	struct SignalO {
		double object;
		char objType;
	};
	struct SOPO {
		ID spID;
		double object;
		char objType;
	};
	struct SP {
		ID sID;
		ID pID;
	};

	struct SPO {
		ID sID;
		ID pID;
		double object;
		char objType;
	};
	enum ResultType {
		SIGNALID,
		OBJECT,
		SUBJECTOBJECT,
		PREDICATEOBJECT,
		SUBJECTPREDICATE,
		SUBJECTPREDICATEOBJECT
	};
	MidResultBuffer(ResultType resultType);
	virtual ~MidResultBuffer();
	void resize(size_t size);
	Status insertSIGNALID(ID id);
	Status insertObject(double object, char objType);
	Status insertSOPO(ID id, double object, char objType);
	Status insertSP(ID subjectID, ID predicateID);
	Status insertSPO(ID subjectID, ID predicateID, double object, char objType);
	size_t getUsedSize() const {
		return usedSize;
	}
	size_t getSize() const {
		return usedSize;
	}

	size_t getCapacity() const {
		return totalSize;
	}
private:
	size_t usedSize;
	size_t totalSize;
	uint sizePerPage;
	uint pos;
	ID* spIDs, *pSpIDs;
	SignalO* objects, *pObjects;
	SOPO* sopos, *pSopos;
	SP* sps, *pSps;
	SPO* spos, *pSpos;
	ResultType resultType;
};

class EntityIDBuffer {
public:
	int IDCount;		//the ID count in a record.
	ID* buffer;
	ID* p;
	int pos;
	size_t usedSize;
	size_t totalSize;
	int sizePerPage;

	//used to sort
	int sortKey;
	bool sorted;
	bool firstTime;
	bool unique;
	//used to hash join;
	vector<int> hashBucket;
	vector<int> prefixSum;
public:
	friend class SortTask;
	friend class HashJoin;
	friend class SortMergeJoin;
	friend class ScanMemory;

	Status insertID(ID id);
	Status getID(ID& id, size_t _pos);
	void empty() {
		hashBucket.clear();
		prefixSum.clear();

		sorted = true;
		firstTime = true;

		p = buffer;
		usedSize = 0;
		pos = 0;
	}

	void setSize(size_t size) {
		usedSize = size * IDCount;
	}

	size_t getUsedSize() const {
		return usedSize;
	}

	void setIDCount(int c) {
		IDCount = c;
	}
	size_t getSize() const {
		if (IDCount != 0)
			return usedSize / IDCount;
		else
			return 0;
	}

	size_t getCapacity() const {
		return totalSize;
	}

	int getTotalPerChunk() const {
		return sizePerPage / IDCount;
	}

	int getIDCount() const {
		return IDCount;
	}

	Status sort(int _sortKey) {
		if ((_sortKey - 1) == sortKey && sorted == true) {
			return OK;
		}

		sortKey = _sortKey - 1;
		return sort();
	}
	Status sort();

	size_t getEntityIDPos(ID id);
	ID* getBuffer() const {
		return buffer;
	}

	Status mergeIntersection(EntityIDBuffer* entBuffer, char* flags,
			ID joinKey);
	Status intersection(EntityIDBuffer* entBuffer, char* flags, ID joinKey1,
			ID joinKey2);
	Status mergeBuffer(EntityIDBuffer* XTemp, EntityIDBuffer* XYTemp);
	static Status mergeSingleBuffer(EntityIDBuffer* entbuffer, ID* buffer1,
			ID* buffer, size_t size1, size_t size2);
	static Status mergeSingleBuffer(EntityIDBuffer*, EntityIDBuffer*,
			EntityIDBuffer*);
	static Status mergeSingleBuffer(EntityIDBuffer*, EntityIDBuffer*);
//	void   uniqe();
	Status mergeBuffer(ID* destBuffer, ID* buffer1, ID* buffer, size_t size1,
			size_t size2, int IDCount, int key);
	Status modifyByFlag(char* flags, int no);
	void getMinMax(ID& min, ID& max);
	void resize(size_t size);
	void resizeForSortMergeJoin(size_t totalSize);
	inline void setSortKey(int _sortKey) {
		if (firstTime == true) {
			sortKey = _sortKey;
			firstTime = false;
			return;
		}
		sortKey = _sortKey;
		sorted = false;
	}
	int getSortKey() const {
		return sortKey;
	}
	bool isInBuffer(ID res[]);
	void print();
	ID getMaxID();
	void initialize(int pageCount = 1);
	EntityIDBuffer();
	virtual ~EntityIDBuffer();

	ID& operator[](const size_t);
	Status appendBuffer(const ID* buffer, size_t size);
	Status appendBuffer1(const ID* buffer, size_t size);
	Status appendBuffer(const EntityIDBuffer* otherBuffer);
	static Status swapBuffer(EntityIDBuffer* &buffer1,
			EntityIDBuffer* &buffer2);
private:
	int partition(ID* p, int first, int last);
	void quickSort(ID*p, int first, int last);
	void quickSort1(ID*p, int first, int last);
	void quickSort(ID* p, int size);
	void merge(int start1, int end1, int start2, int end2, ID* tempBuffer);
	void swapBuffer(ID*& tempBuffer);
	EntityIDBuffer(const EntityIDBuffer* otherBuffer);
	EntityIDBuffer* operator=(const EntityIDBuffer* otherBuffer);
};

#endif /* ENTITYIDBUFFER_H_ */
