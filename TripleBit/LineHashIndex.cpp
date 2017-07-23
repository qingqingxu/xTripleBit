/*
 * LineHashIndex.cpp
 *
 *  Created on: Nov 15, 2010
 *      Author: root
 */

#include "LineHashIndex.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

#include <math.h>
//#define MYDEBUG
/**
 * linear fit;
 * f(x)=kx + b;
 * used to calculate the parameter k and b;
 */
bool LineHashIndex::calculateLineKB(vector<LineHashIndex::Point>& a, double& k, double& b, int pointNo) {
	if (pointNo < 2)
		return false;

	double mX, mY, mXX, mXY;
	mX = mY = mXX = mXY = 0;
	int i;
	for (i = 0; i < pointNo; i++) {
		mX += a[i].x;
		mY += a[i].y;
		mXX += a[i].x * a[i].x;
		mXY += a[i].x * a[i].y;
	}

	if (mX * mX - mXX * pointNo == 0)
		return false;

	k = (mY * mX - mXY * pointNo) / (mX * mX - mXX * pointNo);
	b = (mXY * mX - mY * mXX) / (mX * mX - mXX * pointNo);
	return true;
}

LineHashIndex::LineHashIndex(ChunkManager& _chunkManager, IndexType index_type) :
		chunkManager(_chunkManager), indexType(index_type) {
	idTable = NULL;
	idTableEntries = NULL;
	lineHashIndexBase = NULL;
	startID[0] = startID[1] = startID[2] = startID[3] = 0;
}

LineHashIndex::~LineHashIndex() {
	// TODO Auto-generated destructor stub
	idTable = NULL;
	idTableEntries = NULL;
	startPtr = NULL;
	endPtr = NULL;
	chunkMeta.clear();
	//chunkMeta.shrink_to_fit();
}

/**
 * From startEntry to endEntry in idtableEntries build a line;
 * @param lineNo: the lineNo-th line to be build;
 */
bool LineHashIndex::buildLine(uint startEntry, uint endEntry, uint lineNo) {
	vector<Point> vpt;
	Point pt;
	uint i;

	//build lower limit line;
	for (i = startEntry; i < endEntry; i++) {
		pt.x = idTableEntries[i].min;
		pt.y = i;
		vpt.push_back(pt);
	}

	double ktemp, btemp;
	size_t size = vpt.size();
	if (calculateLineKB(vpt, ktemp, btemp, size) == false)
		return false;

	double difference = btemp; //(vpt[0].y - (ktemp * vpt[0].x + btemp));
	double difference_final = difference;

	for (i = 1; i < size; i++) {
		difference = vpt[i].y - ktemp * vpt[i].x; //vpt[0].y - (ktemp * vpt[0].x + btemp);
		//cout<<"differnce: "<<difference<<endl;
		if ((difference < difference_final) == true)
			difference_final = difference;
	}
	btemp = difference_final;

	lowerk[lineNo] = ktemp;
	lowerb[lineNo] = btemp;
	startID[lineNo] = vpt[0].x;

	vpt.resize(0);
	//build upper limit line;
	for (i = startEntry; i < endEntry; i++) {
		pt.x = idTableEntries[i].max;
		pt.y = i;
		vpt.push_back(pt);
	}

	size = vpt.size();
	calculateLineKB(vpt, ktemp, btemp, size);

	difference = btemp;		//(vpt[0].y - (ktemp * vpt[0].x + btemp));
	difference_final = difference;

	for (i = 1; i < size; i++) {
		difference = vpt[i].y - ktemp * vpt[i].x; //vpt[0].y - (ktemp * vpt[0].x + btemp);
		if (difference > difference_final)
			difference_final = difference;
	}
	btemp = difference_final;

	upperk[lineNo] = ktemp;
	upperb[lineNo] = btemp;
	return true;
}

static ID splitID[3] = { 255, 65535, 16777215 };

Status LineHashIndex::buildIndex() {
#ifdef MYDEBUG
	ofstream out;
	out.open("buildindex", ios::app);
	out << "chunktype: " << chunkType <<endl;
	out.close();
#endif
	if (idTable == NULL) {
		idTable = new MemoryBuffer(HASH_CAPACITY);
		idTableEntries = (TableEntries*) idTable->getBuffer();
		tableSize = 0;
	}

	const uchar* begin, *limit, *reader;

	int lineNo = 0;
	size_t startEntry = 0, endEntry = 0;

	reader = chunkManager.getStartPtr();
	limit = chunkManager.getEndPtr();
	begin = reader;
	if (begin == limit) {
		return OK;
	}

	MetaData* metaData = (MetaData*) reader;
	insertEntries(metaData->min, metaData->max, metaData->pageNo);

	reader = reader + (int) (MemoryBuffer::pagesize - sizeof(ChunkManagerMeta));

	bool isFisrtChunk = true;

	while (reader < limit) {
		isFisrtChunk = false;
		metaData = (MetaData*) reader;
		insertEntries(metaData->min, metaData->max, metaData->pageNo);

		if (metaData->min > splitID[lineNo]) {
			startEntry = endEntry;
			endEntry = tableSize;
			if (buildLine(startEntry, endEntry, lineNo) == true) {
				++lineNo;
			}
		}
		reader = reader + (int) MemoryBuffer::pagesize;
	}

	if (endEntry != tableSize) {
		startEntry = endEntry;
		endEntry = tableSize;
		if (buildLine(startEntry, endEntry, lineNo) == true) {
			++lineNo;
		}
	}

	return OK;
}

bool LineHashIndex::isBufferFull() {
	return tableSize >= idTable->getSize() / sizeof(TableEntries);
}

void LineHashIndex::insertEntries(double min, double max, size_t pageNo) {
	if (isBufferFull()) {
		idTable->resize(HASH_CAPACITY);
		idTableEntries = (TableEntries*) idTable->get_address();
	}
	idTableEntries[tableSize].min = min;
	idTableEntries[tableSize].max = max;
	idTableEntries[tableSize].pageNo = pageNo;

	tableSize++;
}

double LineHashIndex::MetaID(size_t index) {
	assert(index < chunkMeta.size());
	return chunkMeta[index].minx;
}

double LineHashIndex::MetaYID(size_t index) {
	assert(index < chunkMeta.size());
	return chunkMeta[index].miny;

}

size_t LineHashIndex::searchChunkFrank(double id) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << "\ttableSize： " << tableSize << endl;
#endif
	size_t low = 0, mid = 0, high = tableSize - 1;

	if (low == high) {
		return low;
	}
	while (low < high) {
		mid = low + (high - low) / 2;
		while (MetaID(mid) == id) {
			if (mid > 0 && MetaID(mid - 1) < id) {
				return mid - 1;
			}
			if (mid == 0) {
				return mid;
			}
			mid--;
		}
		if (MetaID(mid) < id) {
			low = mid + 1;
		} else if (MetaID(mid) > id) {
			high = mid;
		}
	}
	if (low > 0 && MetaID(low) >= id) {
		return low - 1;
	} else {
		return low;
	}
}

size_t LineHashIndex::searchChunk(double x, double y) {
	if (MetaID(0) > x || tableSize == 0) {
		return 0;
	}

	size_t offsetID = searchChunkFrank(x);
	if (offsetID == tableSize - 1) {
		return offsetID;
	}
	while (offsetID < tableSize - 1) {
		if (MetaID(offsetID + 1) == x) {
			if (MetaYID(offsetID + 1) > y) {
				return offsetID;
			} else {
				offsetID++;
			}
		} else {
			return offsetID;
		}
	}
	return offsetID;
}

bool LineHashIndex::searchChunk(double x, double y, size_t& offsetID)
//return the  exactly which chunk the triple(xID, yID) is in
		{
	if (MetaID(0) > x || tableSize == 0) {
		offsetID = 0;
		return false;
	}

	offsetID = searchChunkFrank(x);
	if (offsetID == tableSize) {
		return false;
	}

	while (offsetID < tableSize - 1) {
		if (MetaID(offsetID + 1) == x) {
			if (MetaYID(offsetID + 1) > y) {
				return true;
			} else {
				offsetID++;
			}
		} else {
			return true;
		}
	}
	return true;
}

bool LineHashIndex::isQualify(size_t offsetId, double x, double y) {
	return (x < MetaID(offsetId + 1) || (x == MetaID(offsetId + 1) && y < MetaYID(offsetId + 1))) && (x > MetaID(offsetId) || (x == MetaID(offsetId) && y >= MetaYID(offsetId)));
}

void LineHashIndex::getOffsetPair(size_t offsetID, unsigned& offsetBegin, unsigned& offsetEnd)
//get the offset of the data begin and end of the offsetIDth Chunk to the startPtr
		{
	if (tableSize == 0) {
		offsetEnd = offsetBegin = sizeof(MetaData);
	}
	offsetBegin = chunkMeta[offsetID].offsetBegin;
	MetaData* metaData = (MetaData*) (startPtr + offsetBegin - sizeof(MetaData));
	offsetEnd = offsetBegin - sizeof(MetaData) + metaData->usedSpace;
}

size_t LineHashIndex::save(MMapBuffer*& indexBuffer)
//tablesize , (startID, lowerk , lowerb, upperk, upperb) * 4
		{
	uchar* writeBuf;
	size_t offset;

	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(), sizeof(size_t) + 16 * sizeof(double) + 4 * sizeof(double));
		writeBuf = indexBuffer->get_address();
		offset = 0;
	} else {
		size_t size = indexBuffer->get_length();
		indexBuffer->resize(size + sizeof(size_t) + 16 * sizeof(double) + 4 * sizeof(double), false);
		writeBuf = indexBuffer->get_address() + size;
		offset = size;
	}

	*(size_t*) writeBuf = tableSize;
	writeBuf += sizeof(size_t);

	for (int i = 0; i < 4; ++i) {
		*(double*) writeBuf = startID[i];
		writeBuf = writeBuf + sizeof(double);

		*(double*) writeBuf = lowerk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*) writeBuf = lowerb[i];
		writeBuf = writeBuf + sizeof(double);

		*(double*) writeBuf = upperk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*) writeBuf = upperb[i];
		writeBuf = writeBuf + sizeof(double);
	}

	indexBuffer->flush();
	delete idTable;
	idTable = NULL;

	return offset;
}

void LineHashIndex::updateLineIndex() {
	uchar* base = lineHashIndexBase;

	*(size_t*) base = tableSize;
	base += sizeof(size_t);

	for (int i = 0; i < 4; ++i) {
		*(double*) base = startID[i];
		base += sizeof(double);

		*(double*) base = lowerk[i];
		base += sizeof(double);
		*(double*) base = lowerb[i];
		base += sizeof(double);

		*(double*) base = upperk[i];
		base += sizeof(double);
		*(double*) base = upperb[i];
		base += sizeof(double);
	}

	delete idTable;
	idTable = NULL;
}

void LineHashIndex::updateChunkMetaData(uint offsetId) {
	//if (offsetId == 0) {
	const uchar* reader = NULL;
	ID subjectID;
	double object;
	char objType;
	reader = startPtr + chunkMeta[offsetId].offsetBegin;
	if (chunkManager.getChunkManagerMeta()->soType == ORDERBYS) {
		reader = Chunk::readID(reader, subjectID);
		reader = Chunk::read(reader, objType, CHAR);
		reader = Chunk::read(reader, object, objType);
		chunkMeta[offsetId].minx = subjectID;
		chunkMeta[offsetId].miny = object;
	} else if (chunkManager.getChunkManagerMeta()->soType == ORDERBYO) {
		reader = Chunk::read(reader, objType, CHAR);
		reader = Chunk::read(reader, object, objType);
		reader = Chunk::readID(reader, subjectID);
		chunkMeta[offsetId].minx = object;
		chunkMeta[offsetId].miny = subjectID;
	}
	//}
}

LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType index_type, uchar*buffer, size_t& offset) {
	LineHashIndex* index = new LineHashIndex(manager, index_type);
	uchar* base = buffer + offset;
	index->lineHashIndexBase = base;

	index->tableSize = *((size_t*) base);
	base = base + sizeof(size_t);

	for (int i = 0; i < 4; ++i) {
		index->startID[i] = *(double*) base;
		base = base + sizeof(double);

		index->lowerk[i] = *(double*) base;
		base = base + sizeof(double);
		index->lowerb[i] = *(double*) base;
		base = base + sizeof(double);

		index->upperk[i] = *(double*) base;
		base = base + sizeof(double);
		index->upperb[i] = *(double*) base;
		base = base + sizeof(double);
	}
	offset = offset + sizeof(size_t) + 16 * sizeof(double) + 4 * sizeof(double);

	//get something useful for the index
	const uchar* reader;
	const uchar* temp;
	register ID subjectID;
	register double object;
	register char objType;
	index->startPtr = index->chunkManager.getStartPtr();
	index->endPtr = index->chunkManager.getEndPtr();
	if (index_type == SUBJECT_INDEX) {
		if (index->startPtr == index->endPtr) {
			index->chunkMeta.push_back( { 0, DBL_MIN, sizeof(MetaData) });
			return index;
		}

		temp = index->startPtr + sizeof(MetaData);

		Chunk::read(Chunk::read(Chunk::readID(temp, subjectID), objType, CHAR), object, objType);
		index->chunkMeta.push_back( { subjectID, object, sizeof(MetaData) });

		reader = index->startPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
		while (reader < index->endPtr) {
			temp = reader + sizeof(MetaData);
			Chunk::read(Chunk::read(Chunk::readID(temp, subjectID), objType, CHAR), object, objType);
			index->chunkMeta.push_back( { subjectID, object, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}
		reader = Chunk::skipBackward(reader, index->endPtr, ORDERBYS);
		if (reader != index->endPtr) {
			Chunk::read(Chunk::read(Chunk::readID(temp, subjectID), objType, CHAR), object, objType);
			index->chunkMeta.push_back( { subjectID, object });
		}

	} else if (index_type == OBJECT_INDEX) {
		if (index->startPtr == index->endPtr) {
			index->chunkMeta.push_back( { DBL_MIN, 0, sizeof(MetaData) });
			return index;
		}

		temp = index->startPtr + sizeof(MetaData);

		Chunk::readID(Chunk::read(Chunk::read(temp, objType, CHAR), object, objType), subjectID);
		index->chunkMeta.push_back( { object, subjectID, sizeof(MetaData) });

		reader = index->startPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
		while (reader < index->endPtr) {
			temp = reader + sizeof(MetaData);
			Chunk::readID(Chunk::read(Chunk::read(temp, objType, CHAR), object, objType), subjectID);
			index->chunkMeta.push_back( { object, subjectID, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}
		reader = Chunk::skipBackward(reader, index->endPtr, ORDERBYS);
		if (reader != index->endPtr) {
			Chunk::readID(Chunk::read(Chunk::read(temp, objType, CHAR), object, objType), subjectID);
			index->chunkMeta.push_back( { object, subjectID });
		}
	}
	return index;
}
