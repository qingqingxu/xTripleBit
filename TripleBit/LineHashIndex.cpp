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
bool LineHashIndex::calculateLineKB(vector<LineHashIndex::Point>& a, double& k, double& b, int pointNo)
{
	if (pointNo < 2)
		return false;

	double mX, mY, mXX, mXY;
	mX = mY = mXX = mXY = 0;
	int i;
	switch (chunkManager.meta->xType) {
	case DataType::BOOL:
		for (i = 0; i < pointNo; i++) {
			mX += a[i].x.var_bool;
			mY += a[i].y;
			mXX += a[i].x.var_bool * a[i].x.var_bool;
			mXY += a[i].x.var_bool * a[i].y;
		}
		break;
	case DataType::CHAR:
		for (i = 0; i < pointNo; i++) {
			mX += a[i].x.var_char;
			mY += a[i].y;
			mXX += a[i].x.var_char * a[i].x.var_char;
			mXY += a[i].x.var_char * a[i].y;
		}
		break;
	case DataType::STRING:
		for (i = 0; i < pointNo; i++) {
			mX += a[i].x.var_uint;
			mY += a[i].y;
			mXX += a[i].x.var_uint * a[i].x.var_uint;
			mXY += a[i].x.var_uint * a[i].y;
		}
		break;
	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
	case DataType::DATE:
		for (i = 0; i < pointNo; i++) {
			mX += a[i].x.var_double;
			mY += a[i].y;
			mXX += a[i].x.var_double * a[i].x.var_double;
			mXY += a[i].x.var_double * a[i].y;
		}
		break;
	default:
		break;
	}


	if (mX * mX - mXX * pointNo == 0)
		return false;

	k = (mY * mX - mXY * pointNo) / (mX * mX - mXX * pointNo);
	b = (mXY * mX - mY * mXX) / (mX * mX - mXX * pointNo);
	return true;
}

LineHashIndex::LineHashIndex(ChunkManager& _chunkManager, IndexType index_type) :
		chunkManager(_chunkManager), indexType(index_type)
{
	idTable = NULL;
	idTableEntries = NULL;
	lineHashIndexBase = NULL;
	switch(chunkManager.meta->xType){
	case DataType::BOOL:
		startID[0].var_bool = startID[1].var_bool = startID[2].var_bool =
				startID[3].var_bool = 1;
		break;
	case DataType::CHAR:
		startID[0].var_char = startID[1].var_char = startID[2].var_char =
				startID[3].var_char = CHAR_MAX;
		break;
	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
	case DataType::DATE:
		startID[0].var_double = startID[1].var_double = startID[2].var_double =
				startID[3].var_double = DBL_MAX;
		break;
	case DataType::STRING:
	default:
		startID[0].var_uint = startID[1].var_uint = startID[2].var_uint =
				startID[3].var_uint = UINT_MAX;
		break;
	}
}

LineHashIndex::~LineHashIndex()
{
	// TODO Auto-generated destructor stub
	idTable = NULL;
	idTableEntries = NULL;
	startPtr = NULL;
	endPtr = NULL;
	chunkMeta.clear();
	swap(chunkMeta, chunkMeta);
}

/**
 * From startEntry to endEntry in idtableEntries build a line;
 * @param lineNo: the lineNo-th line to be build;
 */
bool LineHashIndex::buildLine(uint startEntry, uint endEntry, uint lineNo)
{
	vector<Point> vpt;
	Point pt;
	uint i;

	//build lower limit line;
	for (i = startEntry; i < endEntry; i++)
	{
		pt.x = idTableEntries[i];
		pt.y = i;
		vpt.push_back(pt);
	}

	double ktemp, btemp;
	size_t size = vpt.size();
	if (calculateLineKB(vpt, ktemp, btemp, size) == false)
		return false;

	double difference = btemp; //(vpt[0].y - (ktemp * vpt[0].x + btemp));
	double difference_final = difference;

	for (i = 1; i < size; i++)
	{
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
	for (i = startEntry; i < endEntry; i++)
	{
		pt.x = idTableEntries[i + 1];
		pt.y = i;
		vpt.push_back(pt);
	}

	size = vpt.size();
	calculateLineKB(vpt, ktemp, btemp, size);

	difference = btemp;		//(vpt[0].y - (ktemp * vpt[0].x + btemp));
	difference_final = difference;

	for (i = 1; i < size; i++)
	{
		difference = vpt[i].y - ktemp * vpt[i].x; //vpt[0].y - (ktemp * vpt[0].x + btemp);
		if (difference > difference_final)
			difference_final = difference;
	}
	btemp = difference_final;

	upperk[lineNo] = ktemp;
	upperb[lineNo] = btemp;
	return true;
}

static ID splitID[3] =
{ 255, 65535, 16777215 };

Status LineHashIndex::buildIndex()
{
#ifdef MYDEBUG
	ofstream out;
	out.open("buildindex", ios::app);
	out << "chunktype: " << chunkType <<endl;
	out.close();
#endif
	if (idTable == NULL)
	{
		idTable = new MemoryBuffer(HASH_CAPACITY);
		switch (chunkManager.meta->xType) {
		case DataType::BOOL:
			idTableEntries->var_bool = (bool*) idTable->getBuffer();
			break;
		case DataType::CHAR:
			idTableEntries->var_char = (char*) idTable->getBuffer();
			break;
		case DataType::INT:
		case DataType::UNSIGNED_INT:
		case DataType::DOUBLE:
		case DataType::DATE:
			idTableEntries->var_double = (double*) idTable->getBuffer();
			break;
		case DataType::STRING:
		default:
			idTableEntries->var_uint = (ID*) idTable->getBuffer();
			break;
		}
		tableSize = 0;
	}

	const uchar* begin, *limit, *reader;

	uint lineNo = 0;
	uint startEntry = 0, endEntry = 0;

	varType min;
	reader = chunkManager.getStartPtr;
	limit = chunkManager.getEndPtr;
	begin = reader;
	if (begin == limit) {
		return OK;
	}

	MetaData* metaData = (MetaData*) reader;
	bool isFisrtChunk = true;
	reader = reader + (int) (MemoryBuffer::pagesize - sizeof(ChunkManagerMeta));

	switch (chunkManager.meta->xType) {
	case DataType::BOOL:
		min.var_bool = metaData->minBool;
		insertEntries(min.var_bool);
		while (reader < limit) {
			isFisrtChunk = false;
			metaData = (MetaData*) reader;
			min.var_bool = metaData->minBool;
			insertEntries(min);

			if ((uint) min.var_bool > splitID[lineNo]) {
				startEntry = endEntry;
				endEntry = tableSize;
				if (buildLine(startEntry, endEntry, lineNo) == true) {
					++lineNo;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		const uchar* endPtr = Chunk::skipBackward(reader, isFisrtChunk,
				chunkManager.meta->objType); // get this chunk last <x, y>
		if (endPtr != NULL) {
			min.var_bool = *(bool*) endPtr;
			endPtr = NULL;
		}

		insertEntries(min.var_bool);
		break;
	case DataType::CHAR:
		min.var_char = metaData->minChar;
		insertEntries(min.var_char);
		while (reader < limit) {
			isFisrtChunk = false;
			metaData = (MetaData*) reader;
			min.var_char = metaData->minChar;
			insertEntries(min);

			if ((uint) min.var_char > splitID[lineNo]) {
				startEntry = endEntry;
				endEntry = tableSize;
				if (buildLine(startEntry, endEntry, lineNo) == true) {
					++lineNo;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		const uchar* endPtr = Chunk::skipBackward(reader, isFisrtChunk,
				chunkManager.meta->objType); // get this chunk last <x, y>
		if (endPtr != NULL) {
			min.var_char = *(char*) endPtr;
			endPtr = NULL;
		}

		insertEntries(min.var_char);
		break;
	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
	case DataType::DATE:
		min.var_double = metaData->minDouble;
		insertEntries(min.var_double);
		while (reader < limit) {
			isFisrtChunk = false;
			metaData = (MetaData*) reader;
			min.var_double = metaData->minDouble;
			insertEntries(min);

			if (min.var_double > splitID[lineNo]) {
				startEntry = endEntry;
				endEntry = tableSize;
				if (buildLine(startEntry, endEntry, lineNo) == true) {
					++lineNo;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		const uchar* endPtr = Chunk::skipBackward(reader, isFisrtChunk,
				chunkManager.meta->objType); // get this chunk last <x, y>
		if (endPtr != NULL) {
			min.var_double = *(double*) endPtr;
			endPtr = NULL;
		}

		insertEntries(min.var_double);
		break;

	case DataType::STRING:
	default:
		min.var_uint = metaData->minID;
		insertEntries(min.var_uint);
		while (reader < limit) {
			isFisrtChunk = false;
			metaData = (MetaData*) reader;
			min.var_uint = metaData->minID;
			insertEntries(min);

			if (min.var_uint > splitID[lineNo]) {
				startEntry = endEntry;
				endEntry = tableSize;
				if (buildLine(startEntry, endEntry, lineNo) == true) {
					++lineNo;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		const uchar* endPtr = Chunk::skipBackward(reader, isFisrtChunk,
				chunkManager.meta->objType); // get this chunk last <x, y>
		if (endPtr != NULL) {
			min.var_uint = *(ID*) endPtr;
			endPtr = NULL;
		}

		insertEntries(min.var_uint);
		break;
	}

	startEntry = endEntry;
	endEntry = tableSize;
	if (buildLine(startEntry, endEntry, lineNo) == true) {
		++lineNo;
	}

	return OK;
}

bool LineHashIndex::isBufferFull()
{
	return tableSize >= idTable->getSize() / 4;
}

template<typename T>
void LineHashIndex::insertEntries(T id) {
	switch (chunkManager.meta->xType) {
	case DataType::BOOL:
		if (isBufferFull()) {
			idTable->resize(HASH_CAPACITY);
			idTableEntries->var_bool = (bool*) idTable->get_address();
		}
		idTableEntries[tableSize].var_bool = id;
		tableSize++;
		break;
	case DataType::CHAR:
		if (isBufferFull()) {
			idTableEntries->var_char = (char*) idTable->get_address();
		}
		idTableEntries[tableSize].var_char = id;
		tableSize++;
		break;

	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
	case DataType::DATE:
		if (isBufferFull()) {
			idTableEntries->var_double = (double*) idTable->get_address();
		}
		idTableEntries[tableSize].var_double = id;
		tableSize++;
		break;
	case DataType::STRING:
	default:
		if (isBufferFull()) {
			idTableEntries->var_uint = (ID*) idTable->get_address();
		}
		idTableEntries[tableSize].var_uint = id;
		tableSize++;
		break;
	}
}

varType LineHashIndex::MetaID(size_t index) {
	assert(index < chunkMeta.size());
	return chunkMeta[index].minIDx;
}

varType LineHashIndex::MetaYID(size_t index) {
	assert(index < chunkMeta.size());
	return chunkMeta[index].minIDy;

}

template<typename T>
size_t LineHashIndex::searchChunkFrank(T id)
{
#ifdef MYDEBUG
	cout << __FUNCTION__ << "\ttableSize： " << tableSize <<  endl;
#endif
	size_t low = 0, mid = 0, high = tableSize - 1;

	if (low == high) {
		return low;
	}

	switch (chunkManager.meta->xType) {
	case DataType::BOOL:
		while (low < high) {
			mid = low + (high - low) / 2;
			while (MetaID(mid).var_bool == id) {
				if (mid > 0 && MetaID(mid - 1).var_bool < id) {
					return mid - 1;
				}
				if (mid == 0) {
					return mid;
				}
				mid--;
			}
			if (MetaID(mid).var_bool < id) {
				low = mid + 1;
			} else if (MetaID(mid).var_bool > id) {
				high = mid;
			}
		}
		if (low > 0 && MetaID(low).var_bool >= id) {
			return low - 1;
		} else {
			return low;
		}
		break;
	case DataType::CHAR:
		while (low < high) {
			mid = low + (high - low) / 2;
			while (MetaID(mid).var_char == id) {
				if (mid > 0 && MetaID(mid - 1).var_char < id) {
					return mid - 1;
				}
				if (mid == 0) {
					return mid;
				}
				mid--;
			}
			if (MetaID(mid).var_char < id) {
				low = mid + 1;
			} else if (MetaID(mid).var_char > id) {
				high = mid;
			}
		}
		if (low > 0 && MetaID(low).var_char >= id) {
			return low - 1;
		} else {
			return low;
		}
		break;
	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
	case DataType::DATE:

		while (low < high) {
			mid = low + (high - low) / 2;
			while (MetaID(mid).var_double == id) {
				if (mid > 0 && MetaID(mid - 1).var_double < id) {
					return mid - 1;
				}
				if (mid == 0) {
					return mid;
				}
				mid--;
			}
			if (MetaID(mid).var_double < id) {
				low = mid + 1;
			} else if (MetaID(mid).var_double > id) {
				high = mid;
			}
		}
		if (low > 0 && MetaID(low).var_double >= id) {
			return low - 1;
		} else {
			return low;
		}
		break;
	case DataType::STRING:
	default:
		while (low < high) {
			mid = low + (high - low) / 2;
			while (MetaID(mid).var_uint == id) {
				if (mid > 0 && MetaID(mid - 1).var_uint < id) {
					return mid - 1;
				}
				if (mid == 0) {
					return mid;
				}
				mid--;
			}
			if (MetaID(mid).var_uint < id) {
				low = mid + 1;
			} else if (MetaID(mid).var_uint > id) {
				high = mid;
			}
		}
		if (low > 0 && MetaID(low).var_uint >= id) {
			return low - 1;
		} else {
			return low;
		}
		break;
	}


}

template<typename T>
size_t LineHashIndex::searchChunk(T x, T y){
	if(MetaID(0) > x || tableSize == 0){
		return 0;
	}

	size_t offsetID = searchChunkFrank(x);
	if(offsetID == tableSize-1){
		return offsetID-1;
	}
	while(offsetID < tableSize-2){
		if(MetaID(offsetID+1) == x){
			if(MetaYID(offsetID+1) > y){
				return offsetID;
			}
			else{
				offsetID++;
			}
		}
		else{
			return offsetID;
		}
	}
	return offsetID;
}

template<typename T>
bool LineHashIndex::searchChunk(T x, T y, size_t& offsetID)
//return the  exactly which chunk the triple(xID, yID) is in
{
	if(MetaID(0) > x || tableSize == 0){
		offsetID = 0;
		return false;
	}

	offsetID = searchChunkFrank(x);
	if (offsetID == tableSize-1)
	{
		return false;
	}

	while (offsetID < tableSize - 2)
	{
		if (MetaID(offsetID + 1) == x)
		{
			if (MetaYID(offsetID + 1) > y)
			{
				return true;
			}
			else
			{
				offsetID++;
			}
		}
		else
		{
			return true;
		}
	}
	return true;
}

template<typename T>
bool LineHashIndex::isQualify(size_t offsetId, T x, T y)
{
	return (x < MetaID(offsetId + 1) || (x == MetaID(offsetId + 1) && y < MetaYID(offsetId + 1))) && (x > MetaID(offsetId) || (x == MetaID(offsetId) && y >= MetaYID(offsetId)));
}

void LineHashIndex::getOffsetPair(size_t offsetID, unsigned& offsetBegin, unsigned& offsetEnd)
//get the offset of the data begin and end of the offsetIDth Chunk to the startPtr
{
	if(tableSize == 0){
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

	if (indexBuffer == NULL)
	{
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(),
				sizeof(ID) + 16 * sizeof(double) + 4 * sizeof(ID));
		writeBuf = indexBuffer->get_address();
		offset = 0;
	}
	else
	{
		size_t size = indexBuffer->get_length();
		indexBuffer->resize(size + sizeof(ID) + 16 * sizeof(double) + 4 * sizeof(ID), false);
		writeBuf = indexBuffer->get_address() + size;
		offset = size;
	}

	*(ID*) writeBuf = tableSize;
	writeBuf += sizeof(ID);

	for (int i = 0; i < 4; ++i)
	{
		*(ID*) writeBuf = startID[i];
		writeBuf = writeBuf + sizeof(ID);

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

void LineHashIndex::updateLineIndex()
{
	uchar* base = lineHashIndexBase;

	*(size_t*) base = tableSize;
	base += sizeof(size_t);

	switch (chunkManager.meta->xType) {
	case DataType::BOOL:
		for (int i = 0; i < 4; ++i) {
			*(bool*) base = startID[i];
			base += sizeof(bool);
		}
		break;
	case DataType::CHAR:
		for (int i = 0; i < 4; ++i) {
			*(char*) base = startID[i];
			base += sizeof(char);
		}
		break;
	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
	case DataType::DATE:
		for (int i = 0; i < 4; ++i) {
			*(double*) base = startID[i];
			base += sizeof(double);
		}
		break;
	case DataType::STRING:
	default:
		for (int i = 0; i < 4; ++i) {
			*(uint*) base = startID[i];
			base += sizeof(uint);
		}
		break;
	}

	for (int i = 0; i < 4; ++i)
	{
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

void LineHashIndex::updateChunkMetaData(uint offsetId)
{
	if (offsetId == 0)
	{
		const uchar* reader = NULL;

		varType x,y;
		reader = startPtr + chunkMeta[offsetId].offsetBegin;
		switch (chunkManager.meta->xType) {
		case DataType::BOOL:
			reader = Chunk::read(
					Chunk::read(reader, x.var_bool, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			break;
		case DataType::CHAR:
			reader = Chunk::read(
					Chunk::read(reader, x.var_char, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			break;
		case DataType::INT:
		case DataType::UNSIGNED_INT:
		case DataType::DOUBLE:
		case DataType::DATE:
			reader = Chunk::read(
					Chunk::read(reader, x.var_double, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			break;
		case DataType::STRING:
		default:
			switch (chunkManager.meta->yType) {
			case DataType::BOOL:
				reader = Chunk::read(
						Chunk::read(reader, x.var_uint,
								chunkManager.meta->xType), y.var_bool,
						chunkManager.meta->yType);
				break;
			case DataType::CHAR:
				reader = Chunk::read(
						Chunk::read(reader, x.var_uint,
								chunkManager.meta->xType), y.var_char,
						chunkManager.meta->yType);
				break;
			case DataType::INT:
			case DataType::UNSIGNED_INT:
			case DataType::DOUBLE:
			case DataType::DATE:
				reader = Chunk::read(
						Chunk::read(reader, x.var_uint,
								chunkManager.meta->xType), y.var_double,
						chunkManager.meta->yType);
				break;
			case DataType::STRING:
			default:
			}
			reader = Chunk::read(
					Chunk::read(reader, x.var_uint, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			break;
		}

		chunkMeta[offsetId].minIDx = x;
		chunkMeta[offsetId].minIDy = y;
	}
}

LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType index_type, uchar*buffer,
		size_t& offset)
{
	LineHashIndex* index = new LineHashIndex(manager, index_type);
	uchar* base = buffer + offset;
	index->lineHashIndexBase = base;

	index->tableSize = *((size_t*) base);
	base = base + sizeof(size_t);



	//get something useful for the index
	const uchar* reader;
	const uchar* temp;
	varType x, y;

	switch (chunkManager.meta->xType) {
	case DataType::BOOL:
		for (int i = 0; i < 4; ++i) {
			index->startID[i].var_bool = *(bool*) base;
			base = base + sizeof(bool);

			index->lowerk[i] = *(double*) base;
			base = base + sizeof(double);
			index->lowerb[i] = *(double*) base;
			base = base + sizeof(double);

			index->upperk[i] = *(double*) base;
			base = base + sizeof(double);
			index->upperb[i] = *(double*) base;
			base = base + sizeof(double);
		}
		offset = offset + sizeof(size_t) + 16 * sizeof(double)
				+ 4 * sizeof(bool);
		index->startPtr = index->chunkManager.getStartPtr;
		index->endPtr = index->chunkManager.getEndPtr;
		if (index->startPtr == index->endPtr) {
			x.var_bool = 0;
			y.var_uint = 0;
			index->chunkMeta.push_back( { x, y, sizeof(MetaData) });
			return index;
		}

		temp = index->startPtr + sizeof(MetaData);
		Chunk::read(Chunk::read(temp, x.var_bool, chunkManager.meta->xType),
				y.var_uint, chunkManager.meta->yType);
		index->chunkMeta.push_back( { x, y, sizeof(MetaData) });

		reader = index->startPtr - sizeof(ChunkManagerMeta)
				+ MemoryBuffer::pagesize;
		while (reader < index->endPtr) {
			temp = reader + sizeof(MetaData);
			Chunk::read(Chunk::read(temp, x.var_bool, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			index->chunkMeta.push_back(
					{ x, y, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}
		reader = Chunk::skipBackward(reader, index->endPtr,
				chunkManager.meta->objType);
		Chunk::read(Chunk::read(temp, x.var_bool, chunkManager.meta->xType),
				y.var_uint, chunkManager.meta->yType);
		index->chunkMeta.push_back( { x, y });
		break;
	case DataType::CHAR:
		for (int i = 0; i < 4; ++i) {
			index->startID[i].var_char = *(char*) base;
			base = base + sizeof(char);

			index->lowerk[i] = *(double*) base;
			base = base + sizeof(double);
			index->lowerb[i] = *(double*) base;
			base = base + sizeof(double);

			index->upperk[i] = *(double*) base;
			base = base + sizeof(double);
			index->upperb[i] = *(double*) base;
			base = base + sizeof(double);
		}
		offset = offset + sizeof(size_t) + 16 * sizeof(double)
				+ 4 * sizeof(char);
		index->startPtr = index->chunkManager.getStartPtr;
		index->endPtr = index->chunkManager.getEndPtr;
		if (index->startPtr == index->endPtr) {
			x.var_char = CHAR_MIN;
			y.var_uint = 0;
			index->chunkMeta.push_back( { x, y, sizeof(MetaData) });
			return index;
		}

		temp = index->startPtr + sizeof(MetaData);
		Chunk::read(Chunk::read(temp, x.var_char, chunkManager.meta->xType),
				y.var_uint, chunkManager.meta->yType);
		index->chunkMeta.push_back( { x, y, sizeof(MetaData) });

		reader = index->startPtr - sizeof(ChunkManagerMeta)
				+ MemoryBuffer::pagesize;
		while (reader < index->endPtr) {
			temp = reader + sizeof(MetaData);
			Chunk::read(Chunk::read(temp, x.var_char, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			index->chunkMeta.push_back(
					{ x, y, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}
		reader = Chunk::skipBackward(reader, index->endPtr,
				chunkManager.meta->objType);
		Chunk::read(Chunk::read(temp, x.var_char, chunkManager.meta->xType),
				y.var_uint, chunkManager.meta->yType);
		index->chunkMeta.push_back( { x, y });
		break;
	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
	case DataType::DATE:
		for (int i = 0; i < 4; ++i) {
			index->startID[i].var_double = *(double*) base;
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
		offset = offset + sizeof(size_t) + 16 * sizeof(double)
				+ 4 * sizeof(double);
		index->startPtr = index->chunkManager.getStartPtr;
		index->endPtr = index->chunkManager.getEndPtr;
		if (index->startPtr == index->endPtr) {
			x.var_double = DBL_MIN;
			y.var_uint = 0;
			index->chunkMeta.push_back( { x, y, sizeof(MetaData) });
			return index;
		}

		temp = index->startPtr + sizeof(MetaData);
		Chunk::read(Chunk::read(temp, x.var_double, chunkManager.meta->xType),
				y.var_uint, chunkManager.meta->yType);
		index->chunkMeta.push_back( { x, y, sizeof(MetaData) });

		reader = index->startPtr - sizeof(ChunkManagerMeta)
				+ MemoryBuffer::pagesize;
		while (reader < index->endPtr) {
			temp = reader + sizeof(MetaData);
			Chunk::read(
					Chunk::read(temp, x.var_double, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			index->chunkMeta.push_back(
					{ x, y, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}
		reader = Chunk::skipBackward(reader, index->endPtr,
				chunkManager.meta->objType);
		Chunk::read(Chunk::read(temp, x.var_double, chunkManager.meta->xType),
				y.var_uint, chunkManager.meta->yType);
		index->chunkMeta.push_back( { x, y });
		break;
	case DataType::STRING:
	default:
		for (int i = 0; i < 4; ++i) {
			index->startID[i].var_double = *(uint*) base;
			base = base + sizeof(uint);

			index->lowerk[i] = *(double*) base;
			base = base + sizeof(double);
			index->lowerb[i] = *(double*) base;
			base = base + sizeof(double);

			index->upperk[i] = *(double*) base;
			base = base + sizeof(double);
			index->upperb[i] = *(double*) base;
			base = base + sizeof(double);
		}
		offset = offset + sizeof(size_t) + 16 * sizeof(double)
				+ 4 * sizeof(uint);
		index->startPtr = index->chunkManager.getStartPtr;
		index->endPtr = index->chunkManager.getEndPtr;

		switch (chunkManager.meta->yType) {
		case DataType::BOOL:
			if (index->startPtr == index->endPtr) {
				x.var_uint = 0;
				y.var_bool = 0;
				index->chunkMeta.push_back( { x, y, sizeof(MetaData) });
				return index;
			}

			temp = index->startPtr + sizeof(MetaData);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_bool, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y, sizeof(MetaData) });

			reader = index->startPtr - sizeof(ChunkManagerMeta)
					+ MemoryBuffer::pagesize;
			while (reader < index->endPtr) {
				temp = reader + sizeof(MetaData);
				Chunk::read(
						Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
						y.var_bool, chunkManager.meta->yType);
				index->chunkMeta.push_back(
						{ x, y, reader - index->startPtr + sizeof(MetaData) });

				reader = reader + MemoryBuffer::pagesize;
			}
			reader = Chunk::skipBackward(reader, index->endPtr,
					chunkManager.meta->objType);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_bool, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y });
			break;
		case DataType::CHAR:
			if (index->startPtr == index->endPtr) {
				x.var_uint = 0;
				y.var_char = CHAR_MIN;
				index->chunkMeta.push_back( { x, y, sizeof(MetaData) });
				return index;
			}

			temp = index->startPtr + sizeof(MetaData);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_char, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y, sizeof(MetaData) });

			reader = index->startPtr - sizeof(ChunkManagerMeta)
					+ MemoryBuffer::pagesize;
			while (reader < index->endPtr) {
				temp = reader + sizeof(MetaData);
				Chunk::read(
						Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
						y.var_char, chunkManager.meta->yType);
				index->chunkMeta.push_back(
						{ x, y, reader - index->startPtr + sizeof(MetaData) });

				reader = reader + MemoryBuffer::pagesize;
			}
			reader = Chunk::skipBackward(reader, index->endPtr,
					chunkManager.meta->objType);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_char, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y });
			break;
		case DataType::INT:
		case DataType::UNSIGNED_INT:
		case DataType::DOUBLE:
		case DataType::DATE:
			if (index->startPtr == index->endPtr) {
				x.var_uint = 0;
				y.var_double = DBL_MIN;
				index->chunkMeta.push_back( { x, y, sizeof(MetaData) });
				return index;
			}

			temp = index->startPtr + sizeof(MetaData);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_double, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y, sizeof(MetaData) });

			reader = index->startPtr - sizeof(ChunkManagerMeta)
					+ MemoryBuffer::pagesize;
			while (reader < index->endPtr) {
				temp = reader + sizeof(MetaData);
				Chunk::read(
						Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
						y.var_double, chunkManager.meta->yType);
				index->chunkMeta.push_back(
						{ x, y, reader - index->startPtr + sizeof(MetaData) });

				reader = reader + MemoryBuffer::pagesize;
			}
			reader = Chunk::skipBackward(reader, index->endPtr,
					chunkManager.meta->objType);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_double, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y });
			break;
		case DataType::STRING:
		default:
			if (index->startPtr == index->endPtr) {
				x.var_uint = 0;
				y.var_uint = 0;
				index->chunkMeta.push_back( { x, y, sizeof(MetaData) });
				return index;
			}

			temp = index->startPtr + sizeof(MetaData);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y, sizeof(MetaData) });

			reader = index->startPtr - sizeof(ChunkManagerMeta)
					+ MemoryBuffer::pagesize;
			while (reader < index->endPtr) {
				temp = reader + sizeof(MetaData);
				Chunk::read(
						Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
						y.var_uint, chunkManager.meta->yType);
				index->chunkMeta.push_back(
						{ x, y, reader - index->startPtr + sizeof(MetaData) });

				reader = reader + MemoryBuffer::pagesize;
			}
			reader = Chunk::skipBackward(reader, index->endPtr,
					chunkManager.meta->objType);
			Chunk::read(Chunk::read(temp, x.var_uint, chunkManager.meta->xType),
					y.var_uint, chunkManager.meta->yType);
			index->chunkMeta.push_back( { x, y });
			break;
		}
		break;
	}
	return index;
}
