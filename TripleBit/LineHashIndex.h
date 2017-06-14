/*
 * LineHashIndex.h
 *
 *  Created on: Nov 15, 2010
 *      Author: root
 */

#ifndef LINEHASHINDEX_H_
#define LINEHASHINDEX_H_

class MemoryBuffer;
class ChunkManager;
class MMapBuffer;

#include "TripleBit.h"

class LineHashIndex {
public:
	struct Point{
		varType x;
		uint y;
	};

	struct chunkMetaData
	//except the firstChunk , minIDx, minIDy and offsetBegin will not change with update
	//offsetEnd may change but I think it makes little difference to the result
	//by Frankfan
	{
		varType minIDx;    //The minIDx of a chunk
		varType minIDy;		//The minIDy of a chunk
		uint offsetBegin;	//The beginoffset of a chunk(not include MetaData and relative to the startPtr)
	};

	enum IndexType { SUBJECT_INDEX, OBJECT_INDEX};
private:
	MemoryBuffer* idTable;
	varType* idTableEntries;
	ChunkManager& chunkManager;
	IndexType indexType;
	size_t tableSize;   //chunk number plus 1,because the end edge
	uchar* lineHashIndexBase; //used to do update

	//line parameters;
	double upperk[4];
	double upperb[4];
	double lowerk[4];
	double lowerb[4];

	varType startID[4];

public:
	//some useful thing about the chunkManager
	const uchar *startPtr, *endPtr;
	vector<chunkMetaData> chunkMeta;

private:
	template<typename T>
	void insertEntries(T id);
	template<typename T>
	size_t searchChunkFrank(T id);
	bool buildLine(uint startEntry, uint endEntry, uint lineNo);
	varType MetaID(size_t index);
	varType MetaYID(size_t index);
	static bool calculateLineKB(vector<LineHashIndex::Point>& a, double& k, double& b, int pointNo);
public:
	LineHashIndex(ChunkManager& _chunkManager, IndexType index_type);
	Status buildIndex();
	void getOffsetPair(size_t offsetID, unsigned& offsetBegin, unsigned& offsetEnd);
	template<typename T>
	size_t searchChunk(T x, T y);
	template<typename T>
	bool searchChunk(T x, T y, size_t& offsetID);
	template<typename T>
	bool isQualify(size_t offsetId, T x, T y);
	size_t getTableSize() { return tableSize; }
	size_t save(MMapBuffer*& indexBuffer);
	void saveDelta(MMapBuffer*& indexBuffer, size_t& offset ,const size_t predicateSize);
	virtual ~LineHashIndex();
	void updateChunkMetaData(uint offsetId);
	void updateLineIndex();
private:
	bool isBufferFull();
public:
	static LineHashIndex* load(ChunkManager& manager, IndexType index_type, uchar* buffer, size_t& offset);
};

#endif /* LINEHASHINDEX_H_ */
