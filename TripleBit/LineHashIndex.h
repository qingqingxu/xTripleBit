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
	struct Point {
		double x;
		size_t y;
	};

	struct TableEntries {
		double min, max;
		size_t pageNo;
	};
	struct chunkMetaData
	//except the firstChunk , minIDx, minIDy and offsetBegin will not change with update
	//offsetEnd may change but I think it makes little difference to the result
	//by Frankfan
	{
		double minx;    //The minIDx of a chunk
		double miny;		//The minIDy of a chunk
		uint offsetBegin;	//The beginoffset of a chunk(not include MetaData and relative to the startPtr)
	};

	enum IndexType {
		SUBJECT_INDEX, OBJECT_INDEX
	};
private:
	MemoryBuffer* idTable;
	TableEntries* idTableEntries;
	ChunkManager& chunkManager;
	IndexType indexType;
	size_t tableSize;   //chunk number plus 1,because the end edge
	uchar* lineHashIndexBase; //used to do update

	//line parameters;
	double upperk[4];
	double upperb[4];
	double lowerk[4];
	double lowerb[4];

	double startID[4];

public:
	//some useful thing about the chunkManager
	const uchar *startPtr, *endPtr;
	vector<chunkMetaData> chunkMeta;

private:
	void insertEntries(double min, double max, size_t pageNo);
	size_t searchChunkFrank(double id);
	bool buildLine(uint startEntry, uint endEntry, uint lineNo);
	double MetaID(size_t index);
	double MetaYID(size_t index);
	static bool calculateLineKB(vector<LineHashIndex::Point>& a, double& k, double& b, int pointNo);
public:
	LineHashIndex(ChunkManager& _chunkManager, IndexType index_type);
	Status buildIndex();
	void getOffsetPair(size_t offsetID, unsigned& offsetBegin, unsigned& offsetEnd);
	size_t searchChunk(double x, double y);
	bool searchChunk(double x, double y, size_t& offsetID);
	bool isQualify(size_t offsetId, double x, double y);
	size_t getTableSize() {
		return tableSize;
	}
	size_t save(MMapBuffer*& indexBuffer);
	void saveDelta(MMapBuffer*& indexBuffer, size_t& offset, const size_t predicateSize);
	virtual ~LineHashIndex();
	void updateChunkMetaData(uint offsetId);
	void updateLineIndex();
private:
	bool isBufferFull();
public:
	static LineHashIndex* load(ChunkManager& manager, IndexType index_type, uchar* buffer, size_t& offset);
};

#endif /* LINEHASHINDEX_H_ */
