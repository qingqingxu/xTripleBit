/*
 * ChunkManager.h
 *
 *  Created on: 2010-4-12
 *      Author: liupu
 */

#ifndef CHUNKMANAGER_H_
#define CHUNKMANAGER_H_

class MemoryBuffer;
class MMapBuffer;
class Chunk;
class ChunkManager;

#include "TripleBit.h"
#include "HashIndex.h"
#include "LineHashIndex.h"
#include "ThreadPool.h"

///////////////////////////////////////////////////////////////////////////////////////////////
///// class BitmapBuffer
///////////////////////////////////////////////////////////////////////////////////////////////
class BitmapBuffer {
public:
	map<ID, ChunkManager*> predicate_managers[2];
	ID startColID;
	const string dir;
	MMapBuffer *temp1, *temp2, *temp3, *temp4;
	size_t usedPage1, usedPage2, usedPage3, usedPage4;
public:
	BitmapBuffer(const string dir);
	BitmapBuffer() : startColID(0), dir(DATABASE_PATH) {}
	~BitmapBuffer();
	/// insert a predicate given specified sorting type and predicate id 
	Status insertPredicate(ID predicateID, unsigned char typeID);
	Status insertTriple(ID, ID, unsigned char, ID, unsigned char,bool);
	/// insert a triple;
	Status insertTriple(ID predicateID, ID xID, ID yID, bool isBigger, unsigned char typeID);
	/// get the chunk manager (i.e. the predicate) given the specified type and predicate id
	ChunkManager* getChunkManager(ID, unsigned char);
	/// get the count of chunk manager (i.e. the predicate count) given the specified type
	size_t getSize(unsigned char type) { return predicate_managers[type].size(); }
	void insert(ID predicateID, ID subjectID, ID objectID, bool isBigger, unsigned char flag);

	size_t getTripleCount();

	void flush();
	ID getColCnt() { return startColID; }

	uchar* getPage(unsigned char type, unsigned char flag, size_t& pageNo);
	void save();
	void endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld);
//	static BitmapBuffer* load(const string bitmapBufferDir, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage);
	static BitmapBuffer* load(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage);
private:
	/// get the bytes of a id;
	uchar getBytes(ID id);
	/// get the storage space (in bytes) of a id;
	uchar getLen(ID id);
};

/////////////////////////////////////////////////////////////////////////////////////////////
///////// class ChunkManager
/////////////////////////////////////////////////////////////////////////////////////////////
struct ChunkManagerMeta
{
	size_t length[2];
	size_t usedSpace[2];
	int tripleCount[2];
	unsigned type;
	unsigned pid;
	uchar* startPtr[2];
	uchar* endPtr[2];
};

struct MetaData
{
	ID minID;
	size_t usedSpace;
	bool haveNextPage;
	size_t NextPageNo;
};

class ChunkManager {
private:
	uchar* ptrs[2];

	ChunkManagerMeta* meta;
	///the No. of buffer
	static uint bufferCount;

	///hash index; index the subject and object
	LineHashIndex* chunkIndex[2];

	BitmapBuffer* bitmapBuffer;
	vector<size_t> usedPage[2];
public:
	friend class BuildSortTask;
	friend class BuildMergeTask;
	friend class HashIndex;
	friend class BitmapBuffer;
	friend class PartitionMaster;

public:
	ChunkManager(){}
	ChunkManager(unsigned pid, unsigned _type, BitmapBuffer* bitmapBuffer);
	~ChunkManager();
	Status resize(uchar type);
	Status tripleCountAdd(uchar type) {
		meta->tripleCount[type - 1]++;
		return OK;
	}

	LineHashIndex* getChunkIndex(int type) {
		if(type > 2 || type < 1) {
			return NULL;
		}
		return chunkIndex[type - 1];
	}

	bool isPtrFull(uchar type, unsigned len);

	int getTripleCount() {
		return meta->tripleCount[0] + meta->tripleCount[1];
	}
	int getTripleCount(uchar type) {
			return meta->tripleCount[type - 1];
	}
	unsigned int getPredicateID() const {
		return meta->pid;
	}

	ID getChunkNumber(uchar type);

	void insertXY(unsigned x, unsigned y, unsigned len, uchar type);

	void writeXYId(const uchar* reader, ID x, ID y);

	uchar* getStartPtr(uchar type) {
		return meta->startPtr[type -1];
	}

	uchar* getEndPtr(uchar type) {
		return meta->endPtr[type -1];
	}

	Status buildChunkIndex();
	Status updateChunkIndex();
	static ChunkManager* load(unsigned pid, unsigned type, uchar* buffer, size_t& offset);
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

#include <boost/dynamic_bitset.hpp>

class Chunk {
private:
	uchar type;
	uint count;
	ID xMax;
	ID xMin;
	ID yMax;
	ID yMin;
	ID colStart;
	ID colEnd;
	uchar* startPtr;
	uchar* endPtr;
	vector<bool>* soFlags;
public:
//	boost::dynamic_bitset<> flagVector;
	Chunk(unsigned char, ID, ID, ID, ID, uchar*, uchar*);
	static void writeXId(ID id, uchar*& ptr);
	static void writeYId(ID id, uchar*& ptr);
	~Chunk();
	unsigned int getCount() { return count; }
	void addCount() { count++; }
	bool getSOFlags(uint pos) {
		return (*soFlags)[pos];
	}
	Status setSOFlags(uint pos, bool value) {
		(*soFlags)[pos] = value;
		return OK;
	}
	vector<bool>* getSOFlagsPtr() {
		return soFlags;
	}
	bool isChunkFull() {
		//unsigned char type = this->type;
		return (uint) (endPtr - startPtr + Type_2_Length(type)) > CHUNK_SIZE * getpagesize() ? true : false;
	}
	bool isChunkFull(uchar len) {
			//unsigned char type = this->type;
			return (uint) (endPtr - startPtr + len) > CHUNK_SIZE * getpagesize() ? true : false;
	}
	unsigned char getType() {
		return type;
	}

	/// Read a subject id
	static const uchar* readXId(const uchar* reader, register ID& id);
	/// Read an object id
	static const uchar* readYId(const uchar* reader, register ID& id);
	/// Delete a subject id (just set the id to 0)
	static uchar* deleteXId(uchar* reader);
	/// Delete a object id (just set the id to 0)
	static uchar* deleteYId(uchar* reader);
	/// Skip a s or o
	static const uchar* skipId(const uchar* reader, unsigned char idNums);
	/// Skip backward to s
	static const uchar* skipBackward(const uchar* reader);
	static const uchar* skipBackward(const uchar* reader, const uchar* begin, unsigned type);
	static const uchar* skipForward(const uchar* reader);
	ID getXMax(void) {
		return xMax;
	}
	ID getXMin() {
		return xMin;
	}
	ID getYMax() {
		return yMax;
	}
	ID getYMin() {
		return yMin;
	}
	uchar* getStartPtr() {
		return startPtr;
	}
	uchar* getEndPtr() {
		return endPtr;
	}

	ID getColStart() const {
		return colStart;
	}

	ID getColEnd() const {
		return colEnd;
	}

	void setColStart(ID _colStart) {
		colStart = _colStart;
	}

	void setColEnd(ID _colEnd) {
		colEnd = _colEnd;
	}

	void setStartPtr(uchar* ptr){
		startPtr = ptr;
	}

	void setEndPtr(uchar* ptr) {
		endPtr = ptr;
	}
};
#endif /* CHUNKMANAGER_H_ */
