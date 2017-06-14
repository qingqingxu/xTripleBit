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
//#define MYDEBUG

BitmapBuffer::BitmapBuffer(const string _dir) :
		dir(_dir) {
	// TODO Auto-generated constructor stub
	string filename(dir);
	filename.append("/tempByS");
	tempByS = new MMapBuffer(filename.c_str(),
			INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/tempByS");
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

Status BitmapBuffer::insertPredicate(ID predicateID, OrderByType soType,
		DataType objType = DataType::STRING) {
	predicate_managers[soType][predicateID] = new ChunkManager(predicateID,
			predicateObjTypes[predicateID], soType, this);
}

Status BitmapBuffer::insertTriple(ID predicateID, varType& subject, varType& object,
		OrderByType soType, DataType objType = DataType::STRING) {
	getChunkManager(predicateID, soType)->insertXY(subject, object);
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

uchar* BitmapBuffer::getPage(OrderByType soType, size_t& pageNo) {
	uchar* newPageStartPtr;
	bool tempresize = false;

	if (soType == OrderByType::ORDERBYS) {
		if (usedPageByS * MemoryBuffer::pagesize >= tempByS->getSize()) {
			tempByS->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
			tempresize = true;
		}
		pageNo = usedPageByS;
		newPageStartPtr = tempByS->get_address()
				+ usedPageByS * MemoryBuffer::pagesize;
		usedPageByS++;
	} else if (soType == OrderByType::ORDERBYO) {
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
		if (soType == OrderByType::ORDERBYS) {
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

		} else if (soType == OrderByType::ORDERBYO) {
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

	buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length[0]);

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
	*((SOType*) predicateWriter) = 0;
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
		*((SOType*) predicateWriter) = 0;
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
		*((SOType*) predicateWriter) = 1;
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
	predicateBuffer->flush();

	predicateWriter = predicateBuffer->get_address();

	//update bitmap point address
	ID id;
	for (iter = predicate_managers[0].begin();
			iter != predicate_managers[0].end(); iter++) {
		id = *reinterpret_cast<ID*>(predicateWriter);
		assert(iter->first == id);
		predicateWriter += sizeof(ID) + sizeof(SOType);
		offset = *reinterpret_cast<size_t*>(predicateWriter);
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
		id = *reinterpret_cast<ID*>(predicateWriter);
		assert(iter->first == id);
		predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType);
		offset = *reinterpret_cast<size_t*>(predicateWriter);
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
	buffer->flush();

	//build index;
	MMapBuffer* bitmapIndex = NULL;
	predicateWriter = predicateBuffer->get_address();
#ifdef MYDEBUG
	cout<<"build hash index for subject"<<endl;
	cout << "predicate size: " << predicate_managers[0].size() << endl;
#endif
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin();
			iter != predicate_managers[0].end(); iter++) {
		if (iter->second) {
#ifdef MYDEBUG
			ofstream out;
			out.open("buildindex", ios::app);
			out<< "pid: " <<iter->first<<endl;
			out.close();
#endif
			iter->second->buildChunkIndex();
			LineHashIndex<uint> index = iter->second->getChunkIndex();
			offset = iter->second->getChunkIndex()->save(bitmapIndex);
			predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType)
					+ sizeof(size_t);
			*((size_t*) predicateWriter) = offset;
			predicateWriter = predicateWriter + sizeof(size_t);
		}
	}

#ifdef MYDEBUG
	cout<<"build hash index for object"<<endl;
	cout << "predicate size: " << predicate_managers[1].size() << endl;
#endif
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin();
			iter != predicate_managers[1].end(); iter++) {
		if (iter->second) {
#ifdef MYDEBUG
			ofstream out;
			out.open("buildindex", ios::app);
			out<< "pid: " << iter->first<<endl;
			out.close();
#endif
			iter->second->buildChunkIndex();
			offset = iter->second->getChunkIndex()->save(bitmapIndex);
			predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType)
					+ sizeof(size_t);
			*((size_t*) predicateWriter) = offset;
			predicateWriter = predicateWriter + sizeof(size_t);
		}
	}

	delete bitmapIndex;
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
		id = *reinterpret_cast<ID*>(predicateReader);
		predicateReader += sizeof(ID);
		soType = *reinterpret_cast<SOType*>(predicateReader);
		predicateReader += sizeof(SOType);
		offset = *reinterpret_cast<size_t*>(predicateReader);
		predicateReader += sizeof(size_t);
		indexOffset = *reinterpret_cast<size_t*>(predicateReader);
		predicateReader += sizeof(size_t);
		if (soType == OrderByType::ORDERBYS) {
			ChunkManager *manager = ChunkManager::load(id,
					OrderByType::ORDERBYS, bitmapImage->get_address(), offset);
			manager->chunkIndex = LineHashIndex::load(*manager,
					LineHashIndex::SUBJECT_INDEX,
					bitmapIndexImage->get_address(), indexOffset);
			buffer->predicate_managers[0][id] = manager;
		} else if (soType == OrderByType::ORDERBYO) {
			ChunkManager *manager = ChunkManager::load(id,
					OrderByType::ORDERBYO, bitmapImage->get_address(), offset);
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

		id = *reinterpret_cast<ID*>(predicateReader);
		predicateReader += sizeof(ID);
		soType = *reinterpret_cast<SOType*>(predicateReader);
		predicateReader += sizeof(SOType);
		offset = *reinterpret_cast<size_t*>(predicateReader);
		*((size_t*) predicateReader) = bufferWriterBegin
				- buffer->get_address();
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t); //skip the indexoffset

		startPtr = predicate_managers[soType][id]->getStartPtr;
		offsetId = 0;
		tableSize = predicate_managers[soType][id]->getChunkNumber();
		metaData = (MetaData*) startPtr;

		chunkBegin = startPtr - sizeof(ChunkManagerMeta);
		chunkManagerBegin = chunkBegin;
		memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
		offsetPage++;
		metaDataNew =
				(MetaData*) (bufferWriterBegin + sizeof(ChunkManagerMeta));
		metaDataNew->haveNextPage = false;
		metaDataNew->NextPageNo = 0;

		while (metaData->haveNextPage) {
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
			metaDataNew->haveNextPage = false;
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
			metaDataNew->haveNextPage = false;
			metaDataNew->NextPageNo = 0;
			while (metaData->haveNextPage) {
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
				metaDataNew->haveNextPage = false;
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
			meta->usedSpace[0] = metaDataTemp->usedSpace;
			meta->length[0] = MemoryBuffer::pagesize;
		} else {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterEnd
					- MemoryBuffer::pagesize);
			meta->usedSpace[0] = bufferWriterEnd - bufferWriterBegin
					- sizeof(ChunkManagerMeta) - MemoryBuffer::pagesize
					+ metaDataTemp->usedSpace;
			meta->length[0] = bufferWriterEnd - bufferWriterBegin;
			assert(meta->length[0] % MemoryBuffer::pagesize == 0);
		}
		buffer->flush();

		//not update the LineHashIndex
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	predicateOffset = 0;
	predicateReader = bitmapPredicateImage->get_address();
	while (predicateOffset < sizePredicateBuffer) {
		id = *reinterpret_cast<ID*>(predicateReader);
		predicateReader += sizeof(ID);
		soType = *reinterpret_cast<SOType*>(predicateReader);
		predicateReader += sizeof(SOType);
		offset = *reinterpret_cast<size_t*>(predicateReader);
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t);

#ifdef TTDEBUG
		cout << "id:" << id << " soType:" << soType << endl;
		cout << "offset:" << offset << " indexOffset:" << predicateOffset << endl;
#endif

		uchar *base = buffer->get_address() + offset;
		ChunkManagerMeta *meta = (ChunkManagerMeta*) base;
		meta->startPtr[0] = base + sizeof(ChunkManagerMeta);
		meta->endPtr[0] = meta->startPtr[0] + meta->usedSpace[0];
		meta->startPtr[1] = base + meta->length[0];
		meta->endPtr[1] = meta->startPtr[1] + meta->usedSpace[1];

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

ChunkManager::ChunkManager(ID predicateID, OrderByType soType, DataType objType,
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
	meta->objType = objType;
	if (soType == OrderByType::ORDERBYS) {
		meta->xType = DataType::STRING; //s
		meta->yType = predicateObjTypes[predicateID]; //o
	} else if (soType == OrderByType::ORDERBYS) {
		meta->xType = predicateObjTypes[predicateID]; //o
		meta->yType = DataType::STRING; //s
	}

	if (meta->soType == OrderByType::ORDERBYS) {
		chunkIndex = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX);
	} else if (meta->soType == OrderByType::ORDERBYO) {
		chunkIndex = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX);
	}
}

ChunkManager::~ChunkManager() {
	lastChunkStartPtr = NULL;
	if (chunkIndex != NULL)
		delete chunkIndex;
	chunkIndex = NULL;
}

void ChunkManager::writeXY(const uchar* reader, varType& x, varType& y) {
	Chunk::write(reader, x, meta->xType);
	Chunk::write(reader, y, meta->yType);
}

void ChunkManager::insertXY(varType& x, varType& y) {
	uint len = getLen(meta->xType) + getLen(meta->yType);
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
		resize (type);
		MetaData *metaData = (MetaData*) (meta->endPtr);
		setMetaDataMin(metaData, x, y);
		metaData->haveNextPage = false;
		metaData->NextPageNo = 0;
		metaData->usedSpace = 0;

		writeXY(meta->endPtr + sizeof(MetaData), x, y);
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = meta->length - MemoryBuffer::pagesize
				- sizeof(ChunkManagerMeta) + sizeof(MetaData) + len; // indicate one chunk spare will not save
		tripleCountAdd();
	} else if (meta->usedSpace == 0) {
		MetaData *metaData = (MetaData*) (meta->startPtr);
		memset((char*) metaData, 0, sizeof(MetaData));
		setMetaDataMin(metaData, x, y);
		metaData->haveNextPage = false;
		metaData->NextPageNo = 0;
		metaData->usedSpace = 0;

		writeXY(meta->endPtr + sizeof(MetaData), x, y);
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = sizeof(MetaData) + len;
		tripleCountAdd();
	} else {
		writeXY(meta->endPtr, x, y);
		meta->endPtr = meta->endPtr + len;
		meta->usedSpace = meta->usedSpace + len;
		tripleCountAdd();
	}
}

Status ChunkManager::resize() {
	// TODO
	size_t pageNo = 0;
	lastChunkStartPtr = bitmapBuffer->getPage(meta->soType, pageNo);
	usedPages.push_back(pageNo);
	meta->length = usedPages.size() * MemoryBuffer::pagesize;
	meta->endPtr = lastChunkStartPtr;
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

uint ChunkManager::getLen(DataType dataType){
	switch(dataType){
	case DataType::BOOL:
		return sizeof(bool);
	case DataType::CHAR:
		return sizeof(char);
	case DataType::INT:
	case DataType::UNSIGNED_INT:
	case DataType::DATE:
	case DataType::DOUBLE:
		return sizeof(double);
	case DataType::STRING:
	default:
		return sizeof(ID);
	}
}

size_t ChunkManager::getChunkNumber() {
	return (meta->length) / (MemoryBuffer::pagesize);
}

void ChunkManager::setMetaDataMin(MetaData *metaData, varType& x, varType& y) {
	if (meta->soType == OrderByType::ORDERBYS) {
		metaData->minID = x.var_uint;
	} else if (meta->soType == OrderByType::ORDERBYO) {
		switch (meta->objType) {
		case DataType::BOOL:
			metaData->minBool = y.var_bool;
			break;
		case DataType::CHAR:
			metaData->minChar = y.var_char;
			break;
		case DataType::INT:
		case DataType::DATE:
		case DataType::UNSIGNED_INT:
		case DataType::DOUBLE:
			metaData->minDouble = y.var_double;
			break;
		case DataType::STRING:
		default:
			metaData->minID = y.var_uint;
			break;
		}
	}
}

ChunkManager* ChunkManager::load(ID predicateID, OrderByType soType,
		uchar* buffer, size_t& offset) {
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

void Chunk::writeID(const uchar*& writer, ID data, bool isUpdateAdress){
	*(ID*) writer = data;
	if(isUpdateAdress){
		writer += sizeof(ID);
	}
}

void Chunk::write(const uchar*& writer, varType& data, DataType dataType,
		bool isUpdateAdress) {
	switch (dataType) {
	case DataType::BOOL:
		*(bool*) writer = data.var_bool;
		if (isUpdateAdress) {
			writer += sizeof(bool);
		}
		break;
	case DataType::CHAR:
		*(char*) writer = data.var_char;
		if (isUpdateAdress) {
			writer += sizeof(char);
		}
		break;
	case DataType::INT:
	case DataType::DATE:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
		*(double*) writer = data.var_double;
		if (isUpdateAdress) {
			writer += sizeof(double);
		}
		break;
	case DataType::STRING:
		*(ID*) writer = data.var_uint;
		if (isUpdateAdress) {
			writer += sizeof(ID);
		}
		break;
	default:
		break;
	}
}

const uchar* readID(const uchar* reader, ID& data, bool isUpdateAdress){
	data = *reinterpret_cast<ID*>(reader);
	if(isUpdateAdress){
		reader += sizeof(ID);
	}
	return reader;
}

const uchar* Chunk::read(const uchar* reader, varType& data, DataType dataType,
		bool isUpdateAdress) {
	switch (dataType) {
	case DataType::BOOL:
		data.var_bool = *reinterpret_cast<bool*>(reader);
		if (isUpdateAdress) {
			reader += sizeof(bool);
		}
		break;
	case DataType::CHAR:
		data.var_char = *reinterpret_cast<char*>(reader);
		if (isUpdateAdress) {
			reader += sizeof(char);
		}
		break;
	case DataType::INT:
	case DataType::DATE:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
		data.var_double = *reinterpret_cast<double*>(reader);
		if (isUpdateAdress) {
			reader += sizeof(double);
		}
		break;
	case DataType::STRING:
	default:
		data.var_uint = *reinterpret_cast<ID*>(reader);
		if (isUpdateAdress) {
			reader += sizeof(ID);
		}
		break;
	}
	return reader;
}

uchar* Chunk::deleteData(uchar* reader, DataType dataType) {
	switch (dataType) {
	case DataType::BOOL:
		*(bool*) reader = false;
		reader += sizeof(bool);
		break;
	case DataType::CHAR:
		*(char*) reader = 0;
		reader += sizeof(char);
		break;
	case DataType::INT:
	case DataType::DATE:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
		*(double*) reader = 0;
		reader += sizeof(double);
		break;
	case DataType::STRING:
		*(ID*) reader = 0;
		reader += sizeof(ID);
		break;
	default:
		break;
	}
	return reader;
}

const uchar* Chunk::skipData(const uchar* reader, DataType dataType =
		DataType::STRING) {
	switch (dataType) {
	case DataType::BOOL:
		reader += sizeof(bool);
		break;
	case DataType::CHAR:
		reader += sizeof(char);
		break;
	case DataType::INT:
	case DataType::DATE:
	case DataType::UNSIGNED_INT:
	case DataType::DOUBLE:
		reader += sizeof(double);
		break;
	case DataType::STRING:
		reader += sizeof(ID);
		break;
	default:
		break;
	}
	return reader;
}

const uchar* Chunk::skipForward(const uchar* reader, const uchar* endPtr,
		DataType objType) {
	const char* tempReader = reader;
	while (tempReader < endPtr) {
		if (*tempReader != 0) {
			int xyLen = sizeof(ID) * 2; //x y均为string的id
			switch (objType) {
			case DataType::BOOL:
				xyLen = sizeof(ID) + sizeof(bool);
				break;
			case DataType::CHAR:
				xyLen = sizeof(ID) + sizeof(char);
				break;
			case DataType::INT:
			case DataType::DATE:
			case DataType::UNSIGNED_INT:
			case DataType::DOUBLE:
				xyLen = sizeof(ID) + sizeof(double);
				break;
			case DataType::STRING:
			default:
				break;
			}
			return reader + (tempReader - reader) / xyLen * xyLen;
		}
		tempReader++;
	}
	return NULL;
}

const uchar* Chunk::skipBackward(const uchar* reader, bool isFirstPage,
		DataType objType) {
	const uchar* endPtr = NULL;
	if (isFirstPage) {
		endPtr = reader + (MemoryBuffer::pagesize - sizeof(ChunkManagerMeta));
	} else {
		endPtr = reader + MemoryBuffer::pagesize;
	}
	while (reader + sizeof(MetaData) < endPtr) {
		if (*(--endPtr) != 0) {
			int xyLen = sizeof(ID) * 2; //x y均为string的id
			switch (objType) {
			case DataType::BOOL:
				xyLen = sizeof(ID) + sizeof(bool);
				break;
			case DataType::CHAR:
				xyLen = sizeof(ID) + sizeof(char);
				break;
			case DataType::INT:
			case DataType::DATE:
			case DataType::UNSIGNED_INT:
			case DataType::DOUBLE:
				xyLen = sizeof(ID) + sizeof(double);
				break;
			case DataType::STRING:
			default:
				break;
			}
			return reader + sizeof(MetaData)
					+ (endPtr - (reader + sizeof(MetaData))) / xyLen * xyLen;
		}
	}
	return NULL;
}

const uchar* Chunk::skipBackward(const uchar* reader, const uchar* endPtr,
		DataType objType) {
	const char* tempEndPtr = endPtr;
	while (reader < tempEndPtr) {
		if (*(--tempEndPtr) != 0) {
			int xyLen = sizeof(ID) * 2; //x y均为string的id
			switch (objType) {
			case DataType::BOOL:
				xyLen = sizeof(ID) + sizeof(bool);
				break;
			case DataType::CHAR:
				xyLen = sizeof(ID) + sizeof(char);
				break;
			case DataType::INT:
			case DataType::DATE:
			case DataType::UNSIGNED_INT:
			case DataType::DOUBLE:
				xyLen = sizeof(ID) + sizeof(double);
				break;
			case DataType::STRING:
			default:
				break;
			}
			return reader + sizeof(MetaData)
					+ (tempEndPtr - (reader + sizeof(MetaData))) / xyLen * xyLen;
		}
	}

	return NULL;
}
