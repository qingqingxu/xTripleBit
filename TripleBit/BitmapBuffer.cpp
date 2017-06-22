/*
 * ChunkManager.cpp
 *
 *  Created on: 2010-4-12
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "TempFile.h"
#include "TempMMapBuffer.h"

//#define WORD_ALIGN 1
#define MYDEBUG

BitmapBuffer::BitmapBuffer(const string _dir) :
		dir(_dir) {
	// TODO Auto-generated constructor stub
	string filename(dir);
	filename.append("/tempByS");
	tempByS = new MMapBuffer(filename.c_str(),
			INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/tempByO");
	tempByO = new MMapBuffer(filename.c_str(),
			INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	usedPageByS = usedPageByO = 0;
}

BitmapBuffer::~BitmapBuffer() {
	// TODO Auto-generated destructor stub
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin();
			iter != predicate_managers[0].end(); iter++) {
		if (iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}

	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin();
			iter != predicate_managers[1].end(); iter++) {
		if (iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}
}

Status BitmapBuffer::insertPredicate(ID predicateID, OrderByType soType) {
	predicate_managers[soType][predicateID] = new ChunkManager(predicateID,
			soType, this);
	return OK;
}

Status BitmapBuffer::insertTriple(ID predicateID, ID subjectID, double object,
		OrderByType soType, char objType) {
	getChunkManager(predicateID, soType)->insertXY(subjectID, object, objType);
	return OK;
}

size_t BitmapBuffer::getTripleCount() {
	size_t tripleCount = 0;
	map<ID, ChunkManager*>::iterator begin, limit;
	for (begin = predicate_managers[0].begin(), limit =
			predicate_managers[0].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	tripleCount = 0;
	for (begin = predicate_managers[1].begin(), limit =
			predicate_managers[1].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	return tripleCount;
}

/*
 *	@param id: the chunk manager id ( predicate id );
 *       type: the predicate_manager type;
 */
ChunkManager* BitmapBuffer::getChunkManager(ID predicateID,
		OrderByType soType) {
	//there is no predicate_managers[id]
	if (!predicate_managers[soType].count(predicateID)) {
		//the first time to insert
		insertPredicate(predicateID, soType);
	}
	return predicate_managers[soType][predicateID];
}

void BitmapBuffer::flush() {
	tempByS->flush();
	tempByO->flush();
}

uchar* BitmapBuffer::getPage(bool soType, size_t& pageNo) {
	uchar* newPageStartPtr;
	bool tempresize = false;

	if (soType == ORDERBYS) {
		if (usedPageByS * MemoryBuffer::pagesize >= tempByS->getSize()) {
			tempByS->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
			tempresize = true;
		}
		pageNo = usedPageByS;
		newPageStartPtr = tempByS->get_address()
				+ usedPageByS * MemoryBuffer::pagesize;
		usedPageByS++;
	} else if (soType == ORDERBYO) {
		if (usedPageByO * MemoryBuffer::pagesize >= tempByO->getSize()) {
			tempByO->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
			tempresize = true;
		}
		pageNo = usedPageByO;
		newPageStartPtr = tempByO->get_address()
				+ usedPageByO * MemoryBuffer::pagesize;
		usedPageByO++;
	}

	if (tempresize) {
		if (soType == ORDERBYS) {
			map<ID, ChunkManager*>::iterator iter, limit;
			iter = predicate_managers[0].begin();
			limit = predicate_managers[0].end();
			for (; iter != limit; iter++) {
				if (iter->second == NULL)
					continue;
				iter->second->meta = (ChunkManagerMeta*) (tempByS->get_address()
						+ iter->second->usedPages[0] * MemoryBuffer::pagesize);
				if (iter->second->usedPages.size() == 1) {
					iter->second->meta->endPtr = tempByS->get_address()
							+ iter->second->usedPages.back()
									* MemoryBuffer::pagesize
							+ MemoryBuffer::pagesize
							- (iter->second->meta->length
									- iter->second->meta->usedSpace
									- sizeof(ChunkManagerMeta));
				} else {
					iter->second->meta->endPtr = tempByS->get_address()
							+ iter->second->usedPages.back()
									* MemoryBuffer::pagesize
							+ MemoryBuffer::pagesize
							- (iter->second->meta->length
									- iter->second->meta->usedSpace
									- sizeof(ChunkManagerMeta));
				}
			}

		} else if (soType == ORDERBYO) {
			map<ID, ChunkManager*>::iterator iter, limit;
			iter = predicate_managers[1].begin();
			limit = predicate_managers[1].end();
			for (; iter != limit; iter++) {
				if (iter->second == NULL)
					continue;
				iter->second->meta = (ChunkManagerMeta*) (tempByO->get_address()
						+ iter->second->usedPages[0] * MemoryBuffer::pagesize);
				if (iter->second->usedPages.size() == 1) {
					iter->second->meta->endPtr = tempByO->get_address()
							+ iter->second->usedPages.back()
									* MemoryBuffer::pagesize
							+ MemoryBuffer::pagesize
							- (iter->second->meta->length
									- iter->second->meta->usedSpace
									- sizeof(ChunkManagerMeta));
				} else {
					iter->second->meta->endPtr = tempByO->get_address()
							+ iter->second->usedPages.back()
									* MemoryBuffer::pagesize
							+ MemoryBuffer::pagesize
							- (iter->second->meta->length
									- iter->second->meta->usedSpace
									- sizeof(ChunkManagerMeta));
				}
			}
		}
	}

	return newPageStartPtr;
}

void BitmapBuffer::save() {
	string filename = dir + "/BitmapBuffer";
	MMapBuffer *buffer;
	string bitmapName;
	string predicateFile(filename);
	predicateFile.append("_predicate");

	MMapBuffer *predicateBuffer = new MMapBuffer(predicateFile.c_str(),
			predicate_managers[0].size()
					* (sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);
	uchar *predicateWriter = predicateBuffer->get_address();
	uchar *bufferWriter = NULL;

	map<ID, ChunkManager*>::const_iterator iter = predicate_managers[0].begin();
	size_t offset = 0;

	buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length);

	predicateWriter = predicateBuffer->get_address();
	bufferWriter = buffer->get_address();
	vector<size_t>::iterator pageNoIter = iter->second->usedPages.begin(),
			limit = iter->second->usedPages.end();

	for (; pageNoIter != limit; pageNoIter++) {
		size_t pageNo = *pageNoIter;
		memcpy(bufferWriter,
				tempByS->get_address() + pageNo * MemoryBuffer::pagesize,
				MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;
	}

	*((ID*) predicateWriter) = iter->first;
	predicateWriter += sizeof(ID);
	*((SOType*) predicateWriter) = ORDERBYS;
	predicateWriter += sizeof(SOType);
	*((size_t*) predicateWriter) = offset;
	predicateWriter += sizeof(size_t) * 2;
	offset += iter->second->meta->length;

	bufferWriter = buffer->resize(iter->second->meta->length);
	uchar *startPos = bufferWriter + offset;

	iter++;
	for (; iter != predicate_managers[0].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();

		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos,
					tempByS->get_address() + pageNo * MemoryBuffer::pagesize,
					MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
		}

		*((ID*) predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType*) predicateWriter) = ORDERBYS;
		predicateWriter += sizeof(SOType);
		*((size_t*) predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;

		assert(
				iter->second->usedPages.size() * MemoryBuffer::pagesize
						== iter->second->meta->length);
	}

	buffer->flush();
	tempByS->discard();

	iter = predicate_managers[1].begin();
	for (; iter != predicate_managers[1].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();
		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos,
					tempByO->get_address() + pageNo * MemoryBuffer::pagesize,
					MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
		}

		*((ID*) predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType*) predicateWriter) = ORDERBYO;
		predicateWriter += sizeof(SOType);
		*((size_t*) predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;

		assert(
				iter->second->meta->length
						== iter->second->usedPages.size()
								* MemoryBuffer::pagesize);
	}
	buffer->flush();
	tempByO->discard();
	predicateBuffer->flush();

	/*predicateWriter = predicateBuffer->get_address();

	//update bitmap point address
	ID id;
	for (iter = predicate_managers[0].begin();
			iter != predicate_managers[0].end(); iter++) {
		id = *(ID*) predicateWriter;
		assert(iter->first == id);
		predicateWriter += sizeof(ID) + sizeof(SOType);
		offset = *(size_t*) predicateWriter;
		predicateWriter += sizeof(size_t) * 2;

		uchar *base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*) base;
		iter->second->meta->startPtr = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr = iter->second->meta->startPtr
				+ iter->second->meta->usedSpace;

		//why only update last metadata
		if (iter->second->meta->usedSpace + sizeof(ChunkManagerMeta)
				<= MemoryBuffer::pagesize) {
			MetaData *metaData = (MetaData*) iter->second->meta->startPtr;
			metaData->usedSpace = iter->second->meta->usedSpace;
		} else {
			size_t usedLastPage = (iter->second->meta->usedSpace
					+ sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize;
			if (usedLastPage == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr
						- MemoryBuffer::pagesize);
				metaData->usedSpace = MemoryBuffer::pagesize;
			} else if (usedLastPage > 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr
						- usedLastPage);
				metaData->usedSpace = usedLastPage;
			}
		}
	}

	for (iter = predicate_managers[1].begin();
			iter != predicate_managers[1].end(); iter++) {
		id = *(ID*) predicateWriter;
		assert(iter->first == id);
		predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType);
		offset = *(size_t*) predicateWriter;
		predicateWriter = predicateWriter + sizeof(size_t) * 2;

		uchar *base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*) base;
		iter->second->meta->startPtr = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr = iter->second->meta->startPtr
				+ iter->second->meta->usedSpace;

		if (iter->second->meta->usedSpace + sizeof(ChunkManagerMeta)
				<= MemoryBuffer::pagesize) {
			MetaData *metaData = (MetaData*) (iter->second->meta->startPtr);
			metaData->usedSpace = iter->second->meta->usedSpace;
		} else {
			size_t usedLastPage = (iter->second->meta->usedSpace
					+ sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize;
			if (usedLastPage == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr
						- MemoryBuffer::pagesize);
				metaData->usedSpace = MemoryBuffer::pagesize;
			} else if (usedLastPage > 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr
						- usedLastPage);
				metaData->usedSpace = usedLastPage;
			}
		}
	}
	buffer->flush();*/

	//build index;
/*	MMapBuffer* bitmapIndex = NULL;
	predicateWriter = predicateBuffer->get_address();
#ifdef MYDEBUG
	cout << "build hash index for subject" << endl;
	cout << "predicate size: " << predicate_managers[0].size() << endl;
#endif
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin();
			iter != predicate_managers[0].end(); iter++) {
		if (iter->second) {
			iter->second->buildChunkIndex();
			offset = iter->second->getChunkIndex()->save(bitmapIndex);
			predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType)
					+ sizeof(size_t);
			*((size_t*) predicateWriter) = offset;
			predicateWriter = predicateWriter + sizeof(size_t);
		}
	}

#ifdef MYDEBUG
	cout << "build hash index for object" << endl;
	cout << "predicate size: " << predicate_managers[1].size() << endl;
#endif
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin();
			iter != predicate_managers[1].end(); iter++) {
		if (iter->second) {
			iter->second->buildChunkIndex();
			offset = iter->second->getChunkIndex()->save(bitmapIndex);
			predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType)
					+ sizeof(size_t);
			*((size_t*) predicateWriter) = offset;
			predicateWriter = predicateWriter + sizeof(size_t);
		}
	}*/

	predicateWriter = NULL;
	bufferWriter = NULL;
	//delete bitmapIndex;
	delete buffer;
	delete predicateBuffer;
}

BitmapBuffer *BitmapBuffer::load(MMapBuffer* bitmapImage,
		MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage) {
	BitmapBuffer *buffer = new BitmapBuffer();
	uchar *predicateReader = bitmapPredicateImage->get_address();

	ID id;
	SOType soType;
	size_t offset = 0, indexOffset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	while (predicateOffset < sizePredicateBuffer) {
		id = *(ID*) predicateReader;
		predicateReader += sizeof(ID);
		soType = *(SOType*) predicateReader;
		predicateReader += sizeof(SOType);
		offset = *(size_t*) predicateReader;
		predicateReader += sizeof(size_t);
		indexOffset = *(size_t*) predicateReader;
		predicateReader += sizeof(size_t);
		if (soType == ORDERBYS) {
			ChunkManager *manager = ChunkManager::load(id, ORDERBYS,
					bitmapImage->get_address(), offset);
			manager->chunkIndex = LineHashIndex::load(*manager,
					LineHashIndex::SUBJECT_INDEX,
					bitmapIndexImage->get_address(), indexOffset);
			buffer->predicate_managers[0][id] = manager;
		} else if (soType == ORDERBYO) {
			ChunkManager *manager = ChunkManager::load(id, ORDERBYO,
					bitmapImage->get_address(), offset);
			manager->chunkIndex = LineHashIndex::load(*manager,
					LineHashIndex::OBJECT_INDEX,
					bitmapIndexImage->get_address(), indexOffset);
			buffer->predicate_managers[1][id] = manager;
		}
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	return buffer;
}

void BitmapBuffer::endUpdate(MMapBuffer *bitmapPredicateImage,
		MMapBuffer *bitmapOld) {
	uchar *predicateReader = bitmapPredicateImage->get_address();

	int offsetId = 0, tableSize = 0;
	uchar *startPtr, *bufferWriter, *chunkBegin, *chunkManagerBegin,
			*bufferWriterBegin, *bufferWriterEnd;
	MetaData *metaData = NULL, *metaDataNew = NULL;
	size_t offsetPage = 0, lastoffsetPage = 0;

	ID id = 0;
	SOType soType = 0;
	size_t offset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	string bitmapName = dir + "/BitmapBuffer_Temp";
	MMapBuffer *buffer = new MMapBuffer(bitmapName.c_str(),
			MemoryBuffer::pagesize);

	while (predicateOffset < sizePredicateBuffer) {
		bufferWriter = buffer->get_address()
				+ offsetPage * MemoryBuffer::pagesize;
		lastoffsetPage = offsetPage;
		bufferWriterBegin = bufferWriter;

		id = *(ID*) predicateReader;
		predicateReader += sizeof(ID);
		soType = *(SOType*) predicateReader;
		predicateReader += sizeof(SOType);
		offset = *(size_t*) predicateReader;
		*((size_t*) predicateReader) = bufferWriterBegin
				- buffer->get_address();
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t); //skip the indexoffset

		startPtr = predicate_managers[soType][id]->getStartPtr();
		offsetId = 0;
		tableSize = predicate_managers[soType][id]->getChunkNumber();
		metaData = (MetaData*) startPtr;

		chunkBegin = startPtr - sizeof(ChunkManagerMeta);
		chunkManagerBegin = chunkBegin;
		memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
		offsetPage++;
		metaDataNew =
				(MetaData*) (bufferWriterBegin + sizeof(ChunkManagerMeta));
		metaDataNew->NextPageNo = 0;

		while (metaData->NextPageNo != 0) {
			chunkBegin = TempMMapBuffer::getInstance().getAddress()
					+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			if (metaData->usedSpace == sizeof(MetaData))
				break;
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address()
					+ offsetPage * MemoryBuffer::pagesize;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->NextPageNo = 0;
		}
		offsetId++;
		while (offsetId < tableSize) {
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address()
					+ offsetPage * MemoryBuffer::pagesize;
			chunkBegin = chunkManagerBegin + offsetId * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->NextPageNo = 0;
			while (metaData->NextPageNo != 0) {
				chunkBegin = TempMMapBuffer::getInstance().getAddress()
						+ metaData->NextPageNo * MemoryBuffer::pagesize;
				metaData = (MetaData*) chunkBegin;
				if (metaData->usedSpace == sizeof(MetaData))
					break;
				buffer->resize(MemoryBuffer::pagesize);
				bufferWriter = buffer->get_address()
						+ offsetPage * MemoryBuffer::pagesize;
				memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
				offsetPage++;
				metaDataNew = (MetaData*) bufferWriter;
				metaDataNew->NextPageNo = 0;
			}
			offsetId++;
		}

		bufferWriterEnd = buffer->get_address()
				+ offsetPage * MemoryBuffer::pagesize;
		bufferWriterBegin = buffer->get_address()
				+ lastoffsetPage * MemoryBuffer::pagesize;
		if (offsetPage == lastoffsetPage + 1) {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterBegin
					+ sizeof(ChunkManagerMeta));
			meta->usedSpace = metaDataTemp->usedSpace;
			meta->length = MemoryBuffer::pagesize;
		} else {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterEnd
					- MemoryBuffer::pagesize);
			meta->usedSpace = bufferWriterEnd - bufferWriterBegin
					- sizeof(ChunkManagerMeta) - MemoryBuffer::pagesize
					+ metaDataTemp->usedSpace;
			meta->length = bufferWriterEnd - bufferWriterBegin;
			assert(meta->length % MemoryBuffer::pagesize == 0);
		}
		buffer->flush();

		//not update the LineHashIndex
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	predicateOffset = 0;
	predicateReader = bitmapPredicateImage->get_address();
	while (predicateOffset < sizePredicateBuffer) {
		id = *(ID*) predicateReader;
		predicateReader += sizeof(ID);
		soType = *(SOType*) predicateReader;
		predicateReader += sizeof(SOType);
		offset = *(size_t*) predicateReader;
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t);

#ifdef TTDEBUG
		cout << "id:" << id << " soType:" << soType << endl;
		cout << "offset:" << offset << " indexOffset:" << predicateOffset << endl;
#endif

		uchar *base = buffer->get_address() + offset;
		ChunkManagerMeta *meta = (ChunkManagerMeta*) base;
		meta->startPtr = base + sizeof(ChunkManagerMeta);
		meta->endPtr = meta->startPtr + meta->usedSpace;

		predicate_managers[soType][id]->meta = meta;
		predicate_managers[soType][id]->buildChunkIndex();
		predicate_managers[soType][id]->updateChunkIndex();

		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}
	buffer->flush();

	string bitmapNameOld = dir + "/BitmapBuffer";
//	MMapBuffer *bufferOld = new MMapBuffer(bitmapNameOld.c_str(), 0);
	bitmapOld->discard();
	if (rename(bitmapName.c_str(), bitmapNameOld.c_str()) != 0) {
		perror("rename bitmapName error!");
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void getTempFilename(string& filename, unsigned pid, unsigned _type) {
	filename.clear();
	filename.append(DATABASE_PATH);
	filename.append("temp_");
	char temp[5];
	sprintf(temp, "%d", pid);
	filename.append(temp);
	sprintf(temp, "%d", _type);
	filename.append(temp);
}

ChunkManager::ChunkManager(ID predicateID, OrderByType soType,
		BitmapBuffer* _bitmapBuffer) :
		bitmapBuffer(_bitmapBuffer) {
	usedPages.resize(0);
	size_t pageNo = 0;
	meta = NULL;
	lastChunkStartPtr = bitmapBuffer->getPage(soType, pageNo);
	usedPages.push_back(pageNo);

	meta = (ChunkManagerMeta*) lastChunkStartPtr;
	memset((char*) meta, 0, sizeof(ChunkManagerMeta));
	meta->endPtr = meta->startPtr = lastChunkStartPtr
			+ sizeof(ChunkManagerMeta);
	meta->length = usedPages.size() * MemoryBuffer::pagesize;
	meta->usedSpace = 0;
	meta->tripleCount = 0;
	meta->pid = predicateID;
	meta->soType = soType;

	if (meta->soType == ORDERBYS) {
		chunkIndex = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX);
	} else if (meta->soType == ORDERBYO) {
		chunkIndex = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX);
	}
}

ChunkManager::~ChunkManager() {
	lastChunkStartPtr = NULL;
	if (chunkIndex != NULL)
		delete chunkIndex;
	chunkIndex = NULL;
}

void ChunkManager::writeXY(const uchar* reader, ID x, double y, char objType) {
	if (meta->soType == ORDERBYS) {
		Chunk::writeID(reader, x);
		Chunk::write(reader, objType, CHAR);
		Chunk::write(reader, y, objType);
	} else if (meta->soType == ORDERBYO) {
		Chunk::write(reader, objType, CHAR);
		Chunk::write(reader, y, objType);
		Chunk::writeID(reader, x);
	}
}

uchar* ChunkManager::deleteTriple(uchar* reader, char objType) {
	if (meta->soType == ORDERBYS) {
		*(ID*) reader = 0; //s
		reader += sizeof(ID);
		return Chunk::deleteData(reader, objType); //o
	} else if (meta->soType == ORDERBYO) {
		reader = Chunk::deleteData(reader, objType); //o
		*(ID*) reader = 0; //s
		reader += sizeof(ID);
		return reader;
	}
	return reader; //无操作
}

void ChunkManager::insertXY(ID x, double y, char objType) {
	uint len = sizeof(ID) + sizeof(char) + Chunk::getLen(objType);
	if (isChunkOverFlow(len) == true) {
		if (meta->length == MemoryBuffer::pagesize) {
			MetaData *metaData = (MetaData*) (meta->endPtr - meta->usedSpace);
			metaData->usedSpace = meta->usedSpace;
		} else {
			size_t usedPage =
					MemoryBuffer::pagesize
							- (meta->length - meta->usedSpace
									- sizeof(ChunkManagerMeta));
			MetaData *metaData = (MetaData*) (meta->endPtr - usedPage);
			metaData->usedSpace = usedPage; // every chunk usedspace, include metadata
		}
		size_t pageNo;
		resize(pageNo);

		MetaData *metaData = (MetaData*) (meta->endPtr);
		setMetaDataMin(metaData, x, y);
		metaData->pageNo = pageNo;
		metaData->NextPageNo = 0;
		metaData->usedSpace = 0;

		writeXY(meta->endPtr + sizeof(MetaData), x, y, objType);
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = meta->length - MemoryBuffer::pagesize
				- sizeof(ChunkManagerMeta) + sizeof(MetaData) + len; // indicate one chunk spare will not save
		tripleCountAdd();
	} else if (meta->usedSpace == 0) {
		MetaData *metaData = (MetaData*) (meta->startPtr);
		memset((char*) metaData, 0, sizeof(MetaData));
		setMetaDataMin(metaData, x, y);
		metaData->NextPageNo = usedPages.back();
		metaData->usedSpace = 0;

		writeXY(meta->endPtr + sizeof(MetaData), x, y, objType);
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = sizeof(MetaData) + len;
		tripleCountAdd();
	} else {
		MetaData *metaData;
		if (meta->length == MemoryBuffer::pagesize) {
			metaData = (MetaData*) (meta->endPtr - meta->usedSpace);
		} else {
			size_t usedPage =
					MemoryBuffer::pagesize
							- (meta->length - meta->usedSpace
									- sizeof(ChunkManagerMeta));
			metaData = (MetaData*) (meta->endPtr - usedPage);
		}
		if (meta->soType == ORDERBYS) {
			if (x > metaData->max) {
				metaData->max = x;
			}
		} else if (meta->soType == ORDERBYO) {
			if (y > metaData->max) {
				metaData->max = y;
			}
		}
		writeXY(meta->endPtr, x, y, objType);
		meta->endPtr = meta->endPtr + len;
		meta->usedSpace = meta->usedSpace + len;
		tripleCountAdd();
	}
}

Status ChunkManager::resize(size_t &pageNo) {
	// TODO
	lastChunkStartPtr = bitmapBuffer->getPage(meta->soType, pageNo);
	usedPages.push_back(pageNo);
	meta->length = usedPages.size() * MemoryBuffer::pagesize;
	meta->endPtr = lastChunkStartPtr;
/*
#ifdef MYDEBUG
	ofstream out;
	out.open("ChunkManagerresize", ios::app);
	out << meta->soType << "--------" << meta->pid << "----------"
			<< usedPages.size() << "--------" << meta->length << "--------"
			<< usedPages.size() * MemoryBuffer::pagesize << endl;
	out.close();
#endif
*/
	return OK;
}

/// build the hash index for query;
Status ChunkManager::buildChunkIndex() {
	chunkIndex->buildIndex();
	return OK;
}

/// update the hash index for Query
Status ChunkManager::updateChunkIndex() {
	chunkIndex->updateLineIndex();

	return OK;
}

bool ChunkManager::isChunkOverFlow(uint len) {
	return sizeof(ChunkManagerMeta) + meta->usedSpace + len >= meta->length;
}

void ChunkManager::setMetaDataMin(MetaData *metaData, ID x, double y) {
	if (meta->soType == ORDERBYS) {
		metaData->min = x;
		metaData->max = x;
	} else if (meta->soType == ORDERBYO) {
		metaData->min = y;
		metaData->max = y;
	}
}

size_t ChunkManager::getChunkNumber() {
	return (meta->length) / (MemoryBuffer::pagesize);
}

ChunkManager* ChunkManager::load(ID predicateID, bool soType, uchar* buffer,
		size_t& offset) {
	ChunkManagerMeta * meta = (ChunkManagerMeta*) (buffer + offset);
	if (meta->pid != predicateID || meta->soType != soType) {
		MessageEngine::showMessage("load chunkmanager error: check meta info",
				MessageEngine::ERROR);
		cout << meta->pid << ": " << meta->soType << endl;
		return NULL;
	}

	ChunkManager* manager = new ChunkManager();
	uchar* base = buffer + offset + sizeof(ChunkManagerMeta);
	manager->meta = meta;
	manager->meta->startPtr = base;
	manager->meta->endPtr = manager->meta->startPtr + manager->meta->usedSpace;
	offset = offset + manager->meta->length;

	return manager;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
Chunk::~Chunk() {
	// TODO Auto-generated destructor stub
}

void Chunk::writeID(const uchar*& writer, ID data, bool isUpdateAdress) {
	*(ID*) writer = data;
	if (isUpdateAdress) {
		writer += sizeof(ID);
	}
}

const uchar* Chunk::readID(const uchar* reader, ID& data, bool isUpdateAdress) {
	data = *(ID*) reader;
	if (isUpdateAdress) {
		reader += sizeof(ID);
	}
	return reader;
}

//删除object将数据类型字节改成对应数据类型删除类型
uchar* Chunk::deleteData(uchar* reader, char dataType) {
	switch (dataType) {
	case BOOL:
	case CHAR:
		*(char*) reader = CHAR_DELETE;
		reader += sizeof(char);
		*(char*) reader = 0;
		reader += sizeof(char);
		break;
	case INT:
		*(char*) reader = INT_DELETE;
		reader += sizeof(int);
		*(int*) reader = 0;
		reader += sizeof(int);
		break;
	case FLOAT:
		*(char*) reader = FLOAT_DELETE;
		reader += sizeof(float);
		*(float*) reader = 0;
		reader += sizeof(float);
		break;
	case LONGLONG:
		*(char*) reader = LONGLONG_DELETE;
		reader += sizeof(char);
		*(longlong*) reader = 0;
		reader += sizeof(longlong);
		break;
	case DATE:
	case DOUBLE:
		*(char*) reader = DOUBLE_DELETE;
		reader += sizeof(double);
		*(double*) reader = 0;
		reader += sizeof(double);
		break;
	case UNSIGNED_INT:
	case STRING:
	default:
		*(char*) reader = UNSIGNED_INT_DELETE;
		reader += sizeof(uint);
		*(uint*) reader = 0;
		reader += sizeof(uint);
		break;
	}
	return reader;
}

uint Chunk::getLen(char dataType) {
	int len;
	switch (dataType) {
	case BOOL:
	case CHAR:
		len = sizeof(char);
		break;
	case DATE:
	case DOUBLE:
	case LONGLONG:
		len = sizeof(double);
		break;
	case INT:
	case FLOAT:
	case UNSIGNED_INT:
	case STRING:
	default:
		len = sizeof(uint);
		break;
	}
	return len;
}

const uchar* Chunk::skipData(const uchar* reader, char dataType) {
	return reader + getLen(dataType);
}

Status Chunk::getObjTypeStatus(const uchar*& reader, uint& moveByteNum) {
	char objType = *(char*) reader;
	switch (objType) {
	case NONE:
		return DATA_NONE;
	case BOOL:
	case CHAR:
		reader += sizeof(char) + sizeof(char);
		return DATA_EXSIT;
	case BOOL_DELETE:
	case CHAR_DELETE:
		reader += sizeof(char) + sizeof(char);
		return DATA_DELETE;
	case LONGLONG:
		reader += sizeof(char) + sizeof(longlong);
		return DATA_EXSIT;
	case LONGLONG_DELETE:
		reader += sizeof(char) + sizeof(longlong);
		return DATA_DELETE;
	case DATE:
	case DOUBLE:
		reader += sizeof(char) + sizeof(double);
		return DATA_EXSIT;
	case DATE_DELETE:
	case DOUBLE_DELETE:
		reader += sizeof(char) + sizeof(double);
		return DATA_DELETE;
	case FLOAT_DELETE:
	case INT_DELETE:
	case UNSIGNED_INT_DELETE:
	case STRING_DELETE:
		reader += sizeof(char) + sizeof(uint);
		return DATA_DELETE;
	case FLOAT:
	case INT:
	case UNSIGNED_INT:
	case STRING:
	default:
		reader += sizeof(char) + sizeof(uint);
		return DATA_EXSIT;
	}
}

//若无下一对xy，则返回endPtr表示该Chunk无下一对xy
const uchar* Chunk::skipForward(const uchar* reader, const uchar* endPtr,
		OrderByType soType) {
	if (soType == ORDERBYS) {
		while (reader + sizeof(ID) < endPtr && (*(ID*) reader == 0)) {
			reader += sizeof(ID);
			if (reader + sizeof(char) < endPtr && *(char*) reader != NONE) {
				char objType = *(char*) reader;
				reader += sizeof(char);
				if (reader + getLen(objType) >= endPtr) {
					return endPtr;
				}
			} else {
				return endPtr;
			}
		}
		if (reader + sizeof(ID) >= endPtr) {
			return endPtr;
		} else if (*(ID*) reader != 0) {
			return reader;
		}
	} else if (soType == ORDERBYO) {
		uint moveByteNum = 0;
		int status;
		while (reader + sizeof(char) < endPtr && (*(char*) reader != NONE)) {
			status = getObjTypeStatus(reader, moveByteNum);
			if (status == DATA_NONE) {
				return endPtr;
			} else if (status == DATA_EXSIT) {
				if ((reader += sizeof(ID)) <= endPtr) {
					return reader - moveByteNum;
				} else {
					return endPtr;
				}
			} else if (status == DATA_DELETE) {
				reader += sizeof(ID);
			}
		}
		return endPtr;
	}
	return endPtr;
}

const uchar* Chunk::skipBackward(const uchar* reader, const uchar* endPtr,
		OrderByType soType) {
	const uchar* temp = reader + sizeof(MetaData);
	reader = temp;
	uint len = 0;
	while (reader < endPtr) {
		len = reader - temp;
		reader = Chunk::skipForward(temp, endPtr, soType);
	}
	if (len) {
		return temp - len;
	}
	return endPtr;
}

