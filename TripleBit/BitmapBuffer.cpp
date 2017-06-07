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

unsigned int ChunkManager::bufferCount = 0;

//#define WORD_ALIGN 1
#define MYDEBUG

BitmapBuffer::BitmapBuffer(const string _dir) :
	dir(_dir) {
	// TODO Auto-generated constructor stub
	startColID = 1;
	string filename(dir);
	filename.append("/temp1");
	temp1 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp2");
	temp2 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp3");
	temp3 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp4");
	temp4 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	usedPage1 = usedPage2 = usedPage3 = usedPage4 = 0;
}

BitmapBuffer::~BitmapBuffer() {
	// TODO Auto-generated destructor stub
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++) {
		if (iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}

	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++) {
		if (iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}
}

Status BitmapBuffer::insertPredicate(ID id, unsigned char type) {
	predicate_managers[type][id] = new ChunkManager(id, type, this);
	return OK;
}

size_t BitmapBuffer::getTripleCount() {
	size_t tripleCount = 0;
	map<ID, ChunkManager*>::iterator begin, limit;
	for (begin = predicate_managers[0].begin(), limit = predicate_managers[0].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	tripleCount = 0;
	for (begin = predicate_managers[1].begin(), limit = predicate_managers[1].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	return tripleCount;
}

/*
 *	@param id: the chunk manager id ( predicate id );
 *       type: the predicate_manager type;
 */
ChunkManager* BitmapBuffer::getChunkManager(ID id, unsigned char type) {
	//there is no predicate_managers[id]
	if (!predicate_managers[type].count(id)) {
		//the first time to insert
		insertPredicate(id, type);
	}
	return predicate_managers[type][id];
}

/*
 *	@param f: 0 for triple being sorted by subject; 1 for triple being sorted by object
 *         flag: indicate whether x is bigger than y;
 */
Status BitmapBuffer::insertTriple(ID predicateId, ID xId, ID yId, bool flag, unsigned char f) {
	unsigned char len = 4 * 2;

	if (flag == false) { //s < o
		getChunkManager(predicateId, f)->insertXY(xId, yId, len, 1);
	} else { // s > o, xId: o, yId: s
		getChunkManager(predicateId, f)->insertXY(xId, yId, len, 2);
	}

	//	cout<<getChunkManager(1, 0)->meta->length[0]<<" "<<getChunkManager(1, 0)->meta->tripleCount[0]<<endl;
	return OK;
}

void BitmapBuffer::flush() {
	temp1->flush();
	temp2->flush();
	temp3->flush();
	temp4->flush();
}

unsigned char BitmapBuffer::getBytes(ID id) {
	if (id <= 0xFF) {
		return 1;
	} else if (id <= 0xFFFF) {
		return 2;
	} else if (id <= 0xFFFFFF) {
		return 3;
	} else if (id <= 0xFFFFFFFF) {
		return 4;
	} else {
		return 0;
	}
}

uchar* BitmapBuffer::getPage(uchar type, uchar flag, size_t& pageNo) {
	uchar* rt;
	bool tempresize = false;

	//cout<<__FUNCTION__<<" begin"<<endl;

	if (type == 0) {
		if (flag == 0) { //x < y,first apply 1024*4096, then one page use, when apply memery is not enough, expend 1024*4096
			if (usedPage1 * MemoryBuffer::pagesize >= temp1->getSize()) {
				temp1->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				tempresize = true;
			}
			pageNo = usedPage1;
			rt = temp1->get_address() + usedPage1 * MemoryBuffer::pagesize;
			usedPage1++;
		} else { // x > y
			if (usedPage2 * MemoryBuffer::pagesize >= temp2->getSize()) {
				temp2->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				tempresize = true;
			}
			pageNo = usedPage2;
			rt = temp2->get_address() + usedPage2 * MemoryBuffer::pagesize;
			usedPage2++;
		}
	} else {
		if (flag == 0) {
			if (usedPage3 * MemoryBuffer::pagesize >= temp3->getSize()) {
				temp3->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				tempresize = true;
			}
			pageNo = usedPage3;
			rt = temp3->get_address() + usedPage3 * MemoryBuffer::pagesize;
			usedPage3++;
		} else {
			if (usedPage4 * MemoryBuffer::pagesize >= temp4->getSize()) {
				temp4->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				tempresize = true;
			}
			pageNo = usedPage4;
			rt = temp4->get_address() + usedPage4 * MemoryBuffer::pagesize;
			usedPage4++;
		}
	}

	if (tempresize == true) {
		if (type == 0) {
			if (flag == 0) {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[0].begin();
				limit = predicate_managers[0].end();
				for (; iter != limit; iter++) {
					if (iter->second == NULL)
						continue;
					iter->second->meta = (ChunkManagerMeta*) (temp1->get_address() + iter->second->usedPage[0][0] * MemoryBuffer::pagesize);
					if (iter->second->usedPage[0].size() == 1) {
						iter->second->meta->endPtr[0] = temp1->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[0]
								- iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					} else {
						iter->second->meta->endPtr[0] = temp1->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[0]
								- iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					}
					iter->second->meta->endPtr[1] = temp2->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[1]
							- iter->second->meta->usedSpace[1]);
				}
			} else {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[0].begin();
				limit = predicate_managers[0].end();
				for (; iter != limit; iter++) {
					if (iter->second == NULL)
						continue;
					iter->second->meta->endPtr[1] = temp2->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[1]
							- iter->second->meta->usedSpace[1]);
				}
			}
		} else if (type == 1) {
			if (flag == 0) {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[1].begin();
				limit = predicate_managers[1].end();
				for (; iter != limit; iter++) {
					if (iter->second == NULL)
						continue;
					iter->second->meta = (ChunkManagerMeta*) (temp3->get_address() + iter->second->usedPage[0][0] * MemoryBuffer::pagesize);
					if (iter->second->usedPage[0].size() == 1) {
						iter->second->meta->endPtr[0] = temp3->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[0]
								- iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					} else {
						iter->second->meta->endPtr[0] = temp3->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[0]
								- iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					}
					iter->second->meta->endPtr[1] = temp4->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[1]
							- iter->second->meta->usedSpace[1]);
				}
			} else {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[1].begin();
				limit = predicate_managers[1].end();
				for (; iter != limit; iter++) {
					if (iter->second == NULL)
						continue;
					iter->second->meta->endPtr[1] = temp4->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length[1]
							- iter->second->meta->usedSpace[1]);
				}
			}
		}
	}

	//cout<<__FUNCTION__<<" end"<<endl;

	return rt;
}

unsigned char BitmapBuffer::getLen(ID id) {
	unsigned char len = 0;
	while (id >= 128) {
		len++;
		id >>= 7;
	}
	return len + 1;
}

void BitmapBuffer::save() {
	string filename = dir + "/BitmapBuffer";
	MMapBuffer *buffer;
	string bitmapName;
	string predicateFile(filename);
	predicateFile.append("_predicate");

	MMapBuffer *predicateBuffer = new MMapBuffer(predicateFile.c_str(), predicate_managers[0].size() * (sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);
	uchar *predicateWriter = predicateBuffer->get_address();
	uchar *bufferWriter = NULL;

	map<ID, ChunkManager*>::const_iterator iter = predicate_managers[0].begin();
	size_t offset = 0;

	buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length[0]);

	predicateWriter = predicateBuffer->get_address();
	bufferWriter = buffer->get_address();
	vector<size_t>::iterator pageNoIter = iter->second->usedPage[0].begin(), limit = iter->second->usedPage[0].end();

	for (; pageNoIter != limit; pageNoIter++) {
		size_t pageNo = *pageNoIter;
		memcpy(bufferWriter, temp1->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;
	}

	*((ID*) predicateWriter) = iter->first;
	predicateWriter += sizeof(ID);
	*((SOType*) predicateWriter) = 0;
	predicateWriter += sizeof(SOType);
	*((size_t*) predicateWriter) = offset;
	predicateWriter += sizeof(size_t) * 2;
	offset += iter->second->meta->length[0];

	bufferWriter = buffer->resize(iter->second->meta->length[1]);
	uchar *startPos = bufferWriter + offset;

	pageNoIter = iter->second->usedPage[1].begin();
	limit = iter->second->usedPage[1].end();
	for (; pageNoIter != limit; pageNoIter++) {
		size_t pageNo = *pageNoIter;
		memcpy(startPos, temp2->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		startPos = startPos + MemoryBuffer::pagesize;
	};

	assert(iter->second->meta->length[1] == iter->second->usedPage[1].size() * MemoryBuffer::pagesize);
	offset += iter->second->meta->length[1];

	iter++;
	for (; iter != predicate_managers[0].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length[0]);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[0].begin();
		limit = iter->second->usedPage[0].end();

		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp1->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
		}

		*((ID*) predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType*) predicateWriter) = 0;
		predicateWriter += sizeof(SOType);
		*((size_t*) predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length[0];

		assert(iter->second->usedPage[0].size() * MemoryBuffer::pagesize == iter->second->meta->length[0]);

		bufferWriter = buffer->resize(iter->second->meta->length[1]);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[1].begin();
		limit = iter->second->usedPage[1].end();
		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp2->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}

		offset += iter->second->meta->length[1];
		assert(iter->second->usedPage[1].size() * MemoryBuffer::pagesize == iter->second->meta->length[1]);
	}

	buffer->flush();
	temp1->discard();
	temp2->discard();

	iter = predicate_managers[1].begin();
	for (; iter != predicate_managers[1].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length[0]);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[0].begin();
		limit = iter->second->usedPage[0].end();
		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp3->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
		}

		*((ID*) predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType*) predicateWriter) = 1;
		predicateWriter += sizeof(SOType);
		*((size_t*) predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length[0];

		assert(iter->second->meta->length[0] == iter->second->usedPage[0].size() * MemoryBuffer::pagesize);

		bufferWriter = buffer->resize(iter->second->usedPage[1].size() * MemoryBuffer::pagesize);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[1].begin();
		limit = iter->second->usedPage[1].end();
		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp4->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
		}

		offset += iter->second->meta->length[1];
		assert(iter->second->usedPage[1].size() * MemoryBuffer::pagesize == iter->second->meta->length[1]);
	}
	buffer->flush();
	predicateBuffer->flush();

	predicateWriter = predicateBuffer->get_address();

	//update bitmap point address
	ID id;
	for (iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++) {
		id = *((ID*) predicateWriter);
		assert(iter->first == id);
		predicateWriter += sizeof(ID) + sizeof(SOType);
		offset = *((size_t*) predicateWriter);
		predicateWriter += sizeof(size_t) * 2;

		uchar *base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*) base;
		iter->second->meta->startPtr[0] = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr[0] = iter->second->meta->startPtr[0] + iter->second->meta->usedSpace[0];
		iter->second->meta->startPtr[1] = base + iter->second->meta->length[0];
		iter->second->meta->endPtr[1] = iter->second->meta->startPtr[1] + iter->second->meta->usedSpace[1];

		//why only update last metadata
		if (iter->second->meta->usedSpace[0] + sizeof(ChunkManagerMeta) <= MemoryBuffer::pagesize) {
			MetaData *metaData = (MetaData*) iter->second->meta->startPtr[0];
			metaData->usedSpace = iter->second->meta->usedSpace[0];
		} else {
			size_t usedLastPage = (iter->second->meta->usedSpace[0] + sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize;
			if (usedLastPage == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[0] - MemoryBuffer::pagesize);
				metaData->usedSpace = MemoryBuffer::pagesize;
			} else if (usedLastPage > 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[0] - usedLastPage);
				metaData->usedSpace = usedLastPage;
			}
		}
		if (1) {
			size_t usedLastPage = iter->second->meta->usedSpace[1] % MemoryBuffer::pagesize;
			if (iter->second->meta->usedSpace[1] == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->startPtr[1]);
				metaData->usedSpace = 0;
			} else if (iter->second->meta->usedSpace[1] > 0 && usedLastPage == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[1] - MemoryBuffer::pagesize);
				metaData->usedSpace = MemoryBuffer::pagesize;
			} else if (usedLastPage > 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[1] - usedLastPage);
				metaData->usedSpace = usedLastPage;
			}
		}
	}

	for (iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++) {
		id = *((ID*) predicateWriter);
		assert(iter->first == id);
		predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType);
		offset = *((size_t*) predicateWriter);
		predicateWriter = predicateWriter + sizeof(size_t) * 2;

		uchar *base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*) base;
		iter->second->meta->startPtr[0] = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr[0] = iter->second->meta->startPtr[0] + iter->second->meta->usedSpace[0];
		iter->second->meta->startPtr[1] = base + iter->second->meta->length[0];
		iter->second->meta->endPtr[1] = iter->second->meta->startPtr[1] + iter->second->meta->usedSpace[1];

		if (iter->second->meta->usedSpace[0] + sizeof(ChunkManagerMeta) <= MemoryBuffer::pagesize) {
			MetaData *metaData = (MetaData*) (iter->second->meta->startPtr[0]);
			metaData->usedSpace = iter->second->meta->usedSpace[0];
		} else {
			size_t usedLastPage = (iter->second->meta->usedSpace[0] + sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize;
			if (usedLastPage == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[0] - MemoryBuffer::pagesize);
				metaData->usedSpace = MemoryBuffer::pagesize;
			} else if (usedLastPage > 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[0] - usedLastPage);
				metaData->usedSpace = usedLastPage;
			}
		}
		if (1) {
			size_t usedLastPage = iter->second->meta->usedSpace[1] % MemoryBuffer::pagesize;
			if (iter->second->meta->usedSpace[1] == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->startPtr[1]);
				metaData->usedSpace = 0;
			} else if (iter->second->meta->usedSpace[1] > 0 && usedLastPage == 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[1] - MemoryBuffer::pagesize);
				metaData->usedSpace = MemoryBuffer::pagesize;
			} else if (usedLastPage > 0) {
				MetaData *metaData = (MetaData*) (iter->second->meta->endPtr[1] - usedLastPage);
				metaData->usedSpace = usedLastPage;
			}
		}
	}
	buffer->flush();
	temp3->discard();
	temp4->discard();

	//build index;
	MMapBuffer* bitmapIndex = NULL;
	predicateWriter = predicateBuffer->get_address();
#ifdef MYDEBUG
	cout<<"build hash index for subject"<<endl;
	cout << "predicate size: " << predicate_managers[0].size() << endl;
#endif
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++) {
		if (iter->second) {
#ifdef MYDEBUG
			ofstream out;
			out.open("buildindex", ios::app);
			out<< "pid: " <<iter->first<<endl;
			out.close();
#endif
			iter->second->buildChunkIndex();
			offset = iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
			predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType) + sizeof(size_t);
			*((size_t*) predicateWriter) = offset;
			predicateWriter = predicateWriter + sizeof(size_t);
		}
	}

#ifdef MYDEBUG
	cout<<"build hash index for object"<<endl;
	cout << "predicate size: " << predicate_managers[1].size() << endl;
#endif
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++) {
		if (iter->second) {
#ifdef MYDEBUG
			ofstream out;
			out.open("buildindex", ios::app);
			out<< "pid: " << iter->first<<endl;
			out.close();
#endif
			iter->second->buildChunkIndex();
			offset = iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
			predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType) + sizeof(size_t);
			*((size_t*) predicateWriter) = offset;
			predicateWriter = predicateWriter + sizeof(size_t);
		}
	}

	delete bitmapIndex;
	delete buffer;
	delete predicateBuffer;
}

BitmapBuffer *BitmapBuffer::load(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage) {
	BitmapBuffer *buffer = new BitmapBuffer();
	uchar *predicateReader = bitmapPredicateImage->get_address();

	ID id;
	SOType soType;
	size_t offset = 0, indexOffset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	while (predicateOffset < sizePredicateBuffer) {
		id = *((ID*) predicateReader);
		predicateReader += sizeof(ID);
		soType = *((SOType*) predicateReader);
		predicateReader += sizeof(SOType);
		offset = *((size_t*) predicateReader);
		predicateReader += sizeof(size_t);
		indexOffset = *((size_t*) predicateReader);
		predicateReader += sizeof(size_t);
		if (soType == 0) {
			ChunkManager *manager = ChunkManager::load(id, 0, bitmapImage->get_address(), offset);
			manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, LineHashIndex::YBIGTHANX, bitmapIndexImage->get_address(), indexOffset);
			manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, LineHashIndex::XBIGTHANY, bitmapIndexImage->get_address(), indexOffset);
			buffer->predicate_managers[0][id] = manager;
		} else if (soType == 1) {
			ChunkManager *manager = ChunkManager::load(id, 1, bitmapImage->get_address(), offset);
			manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, LineHashIndex::YBIGTHANX, bitmapIndexImage->get_address(), indexOffset);
			manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, LineHashIndex::XBIGTHANY, bitmapIndexImage->get_address(), indexOffset);
			buffer->predicate_managers[1][id] = manager;
		}
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	return buffer;
}

void BitmapBuffer::endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld) {
	uchar *predicateReader = bitmapPredicateImage->get_address();

	int offsetId = 0, tableSize = 0;
	uchar *startPtr, *bufferWriter, *chunkBegin, *chunkManagerBegin, *bufferWriterBegin, *bufferWriterEnd;
	MetaData *metaData = NULL, *metaDataNew = NULL;
	size_t offsetPage = 0, lastoffsetPage = 0;

	ID id = 0;
	SOType soType = 0;
	size_t offset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	string bitmapName = dir + "/BitmapBuffer_Temp";
	MMapBuffer *buffer = new MMapBuffer(bitmapName.c_str(), MemoryBuffer::pagesize);

	while (predicateOffset < sizePredicateBuffer) {
		bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		lastoffsetPage = offsetPage;
		bufferWriterBegin = bufferWriter;

		id = *((ID*) predicateReader);
		predicateReader += sizeof(ID);
		soType = *((SOType*) predicateReader);
		predicateReader += sizeof(SOType);
		offset = *((size_t*) predicateReader);
		*((size_t*) predicateReader) = bufferWriterBegin - buffer->get_address();
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t); //skip the indexoffset

		//the part of xyType0
		startPtr = predicate_managers[soType][id]->getStartPtr(1);
		offsetId = 0;
		tableSize = predicate_managers[soType][id]->getChunkNumber(1);
		metaData = (MetaData*) startPtr;

		chunkBegin = startPtr - sizeof(ChunkManagerMeta);
		chunkManagerBegin = chunkBegin;
		memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
		offsetPage++;
		metaDataNew = (MetaData*) (bufferWriterBegin + sizeof(ChunkManagerMeta));
		metaDataNew->haveNextPage = false;
		metaDataNew->NextPageNo = 0;

		while (metaData->haveNextPage) {
			chunkBegin = TempMMapBuffer::getInstance().getAddress() + metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			if (metaData->usedSpace == sizeof(MetaData))
				break;
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->haveNextPage = false;
			metaDataNew->NextPageNo = 0;
		}
		offsetId++;
		while (offsetId < tableSize) {
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			chunkBegin = chunkManagerBegin + offsetId * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->haveNextPage = false;
			metaDataNew->NextPageNo = 0;
			while (metaData->haveNextPage) {
				chunkBegin = TempMMapBuffer::getInstance().getAddress() + metaData->NextPageNo * MemoryBuffer::pagesize;
				metaData = (MetaData*) chunkBegin;
				if (metaData->usedSpace == sizeof(MetaData))
					break;
				buffer->resize(MemoryBuffer::pagesize);
				bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
				memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
				offsetPage++;
				metaDataNew = (MetaData*) bufferWriter;
				metaDataNew->haveNextPage = false;
				metaDataNew->NextPageNo = 0;
			}
			offsetId++;
		}

		bufferWriterEnd = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		bufferWriterBegin = buffer->get_address() + lastoffsetPage * MemoryBuffer::pagesize;
		if (offsetPage == lastoffsetPage + 1) {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterBegin + sizeof(ChunkManagerMeta));
			meta->usedSpace[0] = metaDataTemp->usedSpace;
			meta->length[0] = MemoryBuffer::pagesize;
		} else {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterEnd - MemoryBuffer::pagesize);
			meta->usedSpace[0] = bufferWriterEnd - bufferWriterBegin - sizeof(ChunkManagerMeta) - MemoryBuffer::pagesize + metaDataTemp->usedSpace;
			meta->length[0] = bufferWriterEnd - bufferWriterBegin;
			assert(meta->length[0] % MemoryBuffer::pagesize == 0);
		}
		buffer->flush();

		//the part of xyType1
		buffer->resize(MemoryBuffer::pagesize);
		bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		startPtr = predicate_managers[soType][id]->getStartPtr(2);
		offsetId = 0;
		tableSize = predicate_managers[soType][id]->getChunkNumber(2);
		metaData = (MetaData*) startPtr;

		chunkManagerBegin = chunkBegin = startPtr;
		memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
		offsetPage++;
		metaDataNew = (MetaData*) bufferWriter;
		metaDataNew->haveNextPage = false;
		metaDataNew->NextPageNo = 0;

		while (metaData->haveNextPage) {
			chunkBegin = TempMMapBuffer::getInstance().getAddress() + metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			if (metaData->usedSpace == sizeof(MetaData))
				break;
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->haveNextPage = false;
			metaDataNew->NextPageNo = 0;
		}
		offsetId++;
		while (offsetId < tableSize) {
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			chunkBegin = chunkManagerBegin + offsetId * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->haveNextPage = false;
			metaDataNew->NextPageNo = 0;
			while (metaData->haveNextPage) {
				chunkBegin = TempMMapBuffer::getInstance().getAddress() + metaData->NextPageNo * MemoryBuffer::pagesize;
				metaData = (MetaData*) chunkBegin;
				if (metaData->usedSpace == sizeof(MetaData))
					break;
				buffer->resize(MemoryBuffer::pagesize);
				bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
				memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
				offsetPage++;
				metaDataNew = (MetaData*) bufferWriter;
				metaDataNew->haveNextPage = false;
				metaDataNew->NextPageNo = 0;
			}
			offsetId++;
		}

		bufferWriterEnd = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		bufferWriterBegin = buffer->get_address() + lastoffsetPage * MemoryBuffer::pagesize;
		if (1) {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterEnd - MemoryBuffer::pagesize);
			meta->length[1] = bufferWriterEnd - bufferWriterBegin - meta->length[0];
			meta->usedSpace[1] = meta->length[1] - MemoryBuffer::pagesize + metaDataTemp->usedSpace;
			//not update the startPtr, endPtr
		}
		buffer->flush();
		//not update the LineHashIndex
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	predicateOffset = 0;
	predicateReader = bitmapPredicateImage->get_address();
	while (predicateOffset < sizePredicateBuffer) {
		id = *((ID*) predicateReader);
		predicateReader += sizeof(ID);
		soType = *((SOType*) predicateReader);
		predicateReader += sizeof(SOType);
		offset = *((size_t*) predicateReader);
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

ChunkManager::ChunkManager(unsigned pid, unsigned _type, BitmapBuffer* _bitmapBuffer) :
	bitmapBuffer(_bitmapBuffer) {
	usedPage[0].resize(0);
	usedPage[1].resize(0);
	size_t pageNo = 0;
	meta = NULL;
	ptrs[0] = bitmapBuffer->getPage(_type, 0, pageNo);
	usedPage[0].push_back(pageNo);
	ptrs[1] = bitmapBuffer->getPage(_type, 1, pageNo);
	usedPage[1].push_back(pageNo);

	assert(ptrs[1] != ptrs[0]);

	meta = (ChunkManagerMeta*) ptrs[0];
	memset((char*) meta, 0, sizeof(ChunkManagerMeta));
	meta->endPtr[0] = meta->startPtr[0] = ptrs[0] + sizeof(ChunkManagerMeta);
	meta->endPtr[1] = meta->startPtr[1] = ptrs[1];
	meta->length[0] = usedPage[0].size() * MemoryBuffer::pagesize;
	meta->length[1] = usedPage[1].size() * MemoryBuffer::pagesize;
	meta->usedSpace[0] = 0;
	meta->usedSpace[1] = 0;
	meta->tripleCount[0] = meta->tripleCount[1] = 0;
	meta->pid = pid;
	meta->type = _type;

	//need to modify!
	if (meta->type == 0) {
		chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX, LineHashIndex::YBIGTHANX);
		chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX, LineHashIndex::XBIGTHANY);
	} else {
		chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX, LineHashIndex::YBIGTHANX);
		chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX, LineHashIndex::XBIGTHANY);
	}

	for (int i = 0; i < 2; i++)
		meta->tripleCount[i] = 0;
}

ChunkManager::~ChunkManager() {
	// TODO Auto-generated destructor stub
	///free the buffer;
	ptrs[0] = ptrs[1] = NULL;

	if (chunkIndex[0] != NULL)
		delete chunkIndex[0];
	chunkIndex[0] = NULL;
	if (chunkIndex[1] != NULL)
		delete chunkIndex[1];
	chunkIndex[1] = NULL;
}

void ChunkManager::writeXYId(const uchar* reader, ID x, ID y){
	*((ID*) reader) = x;
	reader += sizeof(ID);
	*((ID*) reader) = y;
	reader -= sizeof(ID);//go back
}

void ChunkManager::insertXY(unsigned x, unsigned y, unsigned len, uchar type)
{
	if (isPtrFull(type, len) == true) {
		if (type == 1) {
			if (meta->length[0] == MemoryBuffer::pagesize) {
				MetaData *metaData = (MetaData*) (meta->endPtr[0] - meta->usedSpace[0]);
				metaData->usedSpace = meta->usedSpace[0];
			} else {
				size_t usedPage = MemoryBuffer::pagesize - (meta->length[0] - meta->usedSpace[0] - sizeof(ChunkManagerMeta));
				MetaData *metaData = (MetaData*) (meta->endPtr[0] - usedPage);
				metaData->usedSpace = usedPage;// every chunk usedspace, include metadata
			}
			resize(type);
			MetaData *metaData = (MetaData*) (meta->endPtr[0]);
			metaData->minID = x;
			metaData->haveNextPage = false;
			metaData->NextPageNo = 0;
			metaData->usedSpace = 0;

			writeXYId(meta->endPtr[0] + sizeof(MetaData), x, y);
			//memcpy(meta->endPtr[0] + sizeof(MetaData), temp, len);
			meta->endPtr[0] = meta->endPtr[0] + sizeof(MetaData) + len;
			meta->usedSpace[0] = meta->length[0] - MemoryBuffer::pagesize - sizeof(ChunkManagerMeta) + sizeof(MetaData) + len;// indicate one chunk spare will not save
			tripleCountAdd(type);
		} else {
			size_t usedPage = MemoryBuffer::pagesize - (meta->length[1] - meta->usedSpace[1]);
			MetaData *metaData = (MetaData*) (meta->endPtr[1] - usedPage);
			metaData->usedSpace = usedPage;

			resize(type);
			metaData = (MetaData*) (meta->endPtr[1]);
			metaData->minID = y;
			metaData->haveNextPage = false;
			metaData->NextPageNo = 0;
			metaData->usedSpace = 0;

			writeXYId(meta->endPtr[1] + sizeof(MetaData), x, y);
			//memcpy(meta->endPtr[1] + sizeof(MetaData), temp, len);
			meta->endPtr[1] = meta->endPtr[1] + sizeof(MetaData) + len;
			meta->usedSpace[1] = meta->length[1] - MemoryBuffer::pagesize + sizeof(MetaData) + len;
			tripleCountAdd(type);
		}
	} else if (meta->usedSpace[type - 1] == 0) {
		MetaData *metaData = (MetaData*) (meta->startPtr[type - 1]);
		memset((char*) metaData, 0, sizeof(MetaData));
		metaData->minID = ((type == 1) ? x : y);
		metaData->haveNextPage = false;
		metaData->NextPageNo = 0;
		metaData->usedSpace = 0;

		writeXYId(meta->endPtr[type - 1] + sizeof(MetaData), x, y);
		meta->endPtr[type - 1] = meta->endPtr[type - 1] + sizeof(MetaData) + len;
		meta->usedSpace[type - 1] = sizeof(MetaData) + len;
		tripleCountAdd(type);
	} else {
		writeXYId(meta->endPtr[type - 1], x, y);

		meta->endPtr[type - 1] = meta->endPtr[type - 1] + len;
		meta->usedSpace[type - 1] = meta->usedSpace[type - 1] + len;
		tripleCountAdd(type);
	}
}

Status ChunkManager::resize(uchar type) {
	// TODO
	size_t pageNo = 0;
	ptrs[type - 1] = bitmapBuffer->getPage(meta->type, type - 1, pageNo);
	usedPage[type - 1].push_back(pageNo);
	meta->length[type - 1] = usedPage[type - 1].size() * MemoryBuffer::pagesize;
	meta->endPtr[type - 1] = ptrs[type - 1];

	bufferCount++;
	return OK;
}

/// build the hash index for query;
Status ChunkManager::buildChunkIndex() {
	chunkIndex[0]->buildIndex(1);
	chunkIndex[1]->buildIndex(2);

	return OK;
}

/// update the hash index for Query
Status ChunkManager::updateChunkIndex() {
	chunkIndex[0]->updateLineIndex();
	chunkIndex[1]->updateLineIndex();

	return OK;
}

bool ChunkManager::isPtrFull(unsigned char type, unsigned len) {
	if (type == 1) {
		len = len + sizeof(ChunkManagerMeta);
	}
	return meta->usedSpace[type - 1] + len >= meta->length[type - 1];
}

ID ChunkManager::getChunkNumber(unsigned char type) {
	return (meta->length[type - 1]) / (MemoryBuffer::pagesize);
}

ChunkManager* ChunkManager::load(unsigned pid, unsigned type, uchar* buffer, size_t& offset) {
	ChunkManagerMeta * meta = (ChunkManagerMeta*) (buffer + offset);
	if (meta->pid != pid || meta->type != type) {
		MessageEngine::showMessage("load chunkmanager error: check meta info", MessageEngine::ERROR);
		cout << meta->pid << ": " << meta->type << endl;
		return NULL;
	}

	ChunkManager* manager = new ChunkManager();
	uchar* base = buffer + offset + sizeof(ChunkManagerMeta);
	manager->meta = meta;
	manager->meta->startPtr[0] = base;
	manager->meta->startPtr[1] = buffer + offset + manager->meta->length[0];
	manager->meta->endPtr[0] = manager->meta->startPtr[0] + manager->meta->usedSpace[0];
	manager->meta->endPtr[1] = manager->meta->startPtr[1] + manager->meta->usedSpace[1];

	offset = offset + manager->meta->length[0] + manager->meta->length[1];

	return manager;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

Chunk::Chunk(uchar type, ID xMax, ID xMin, ID yMax, ID yMin, uchar* startPtr, uchar* endPtr) {
	// TODO Auto-generated constructor stub
	this->type = type;
	this->xMax = xMax;
	this->xMin = xMin;
	this->yMax = yMax;
	this->yMin = yMin;
	count = 0;
	this->startPtr = startPtr;
	this->endPtr = endPtr;
}

Chunk::~Chunk() {
	// TODO Auto-generated destructor stub
	this->startPtr = 0;
	this->endPtr = 0;
}

/*
 *	write x id; set the 7th bit to 0 to indicate it is a x byte;
 */
void Chunk::writeXId(ID id, uchar*& ptr) {
	// Write a id
	while (id >= 128) {
		unsigned char c = static_cast<unsigned char> (id & 127);
		*ptr = c;
		ptr++;
		id >>= 7;
	}
	*ptr = static_cast<unsigned char> (id & 127);
	ptr++;
}

/*
 *	write y id; set the 7th bit to 1 to indicate it is a y byte;
 */
void Chunk::writeYId(ID id, uchar*& ptr) {
	while (id >= 128) {
		uchar c = static_cast<uchar> (id | 128);
		*ptr = c;
		ptr++;
		id >>= 7;
	}
	*ptr = static_cast<uchar> (id | 128);
	ptr++;
}

static inline unsigned int readUInt(const uchar* reader) {
	return (reader[0] << 24 | reader[1] << 16 | reader[2] << 8 | reader[3]);
}

const uchar* Chunk::readXId(const uchar* reader, register ID& id) {
	id = *(ID*)reader;
	reader += 4;
	return reader;
}

const uchar* Chunk::readYId(const uchar* reader, register ID& id) {
	// Read an y id
	id = *(ID*)reader;
	reader += 4;
	return reader;
}

uchar* Chunk::deleteXId(uchar* reader)
/// Delete a subject id (just set the id to 0)
{
	register unsigned int c;

	while (true) {
		c = *reader;
		if (!(c & 128))
			(*reader) = 0;
		else
			break;
		reader++;
	}
	return reader;
}

uchar* Chunk::deleteYId(uchar* reader)
/// Delete an object id (just set the id to 0)
{
	register unsigned int c;

	while (true) {
		c = *reader;
		if (c & 128)
			(*reader) = (*reader) & 0x80;
		else
			break;
		reader++;
	}
	return reader;
}

const uchar* Chunk::skipId(const uchar* reader, uchar idNums) {
	// Skip an id
	for(int i = 0; i < idNums; i++){
		reader += 4;
	}

	return reader;
}

const uchar* Chunk::skipForward(const uchar* reader) {
	// skip a x,y forward;
	return skipId(reader, 2);
}

const uchar* Chunk::skipBackward(const uchar* reader) {
	// skip backward to the last x,y;
	while ((ID*)reader == 0) //last y
		reader -= 4;
	reader -= 4; //last x
	return reader;
}

const uchar* Chunk::skipBackward(const uchar* reader, const uchar* begin, unsigned type) {
	//if is the begin of One Chunk

	if (type == 1) {
			if ((reader - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize == 0 || (reader + 1 - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize
					== 0) {
				if ((reader - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) == MemoryBuffer::pagesize || (reader + 1 - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta))
						== MemoryBuffer::pagesize)
				// if is the second Chunk
				{
					reader = begin;
					MetaData* metaData = (MetaData*) reader;
					reader = reader + metaData->usedSpace;
					--reader;
					return skipBackward(reader);
				}
				reader = begin - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize * ((reader - begin + sizeof(ChunkManagerMeta)) / MemoryBuffer::pagesize - 1);
				MetaData* metaData = (MetaData*) reader;
				reader = reader + metaData->usedSpace;
				--reader;
				return skipBackward(reader);
			} else if (reader <= begin + sizeof(MetaData)) {
				return begin - 1;
			} else {
				//if is not the begin of one Chunk
				return skipBackward(reader);
			}
		}
		if (type == 2) {
			if ((reader - begin - sizeof(MetaData)) % MemoryBuffer::pagesize == 0 || (reader + 1 - begin - sizeof(MetaData)) % MemoryBuffer::pagesize == 0) {
				reader = begin + MemoryBuffer::pagesize * ((reader - begin) / MemoryBuffer::pagesize - 1);
				MetaData* metaData = (MetaData*) reader;
				reader = reader + metaData->usedSpace;
				--reader;
				return skipBackward(reader);
			} else if (reader <= begin + sizeof(MetaData)) {
				return begin - 1;
			} else {
				//if is not the begin of one Chunk
				return skipBackward(reader);
			}
		}
	/*if (type == 1) {
		if ((reader - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize == 0 || (reader + 1 - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize
				== 0) {
			if ((reader - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) == MemoryBuffer::pagesize || (reader + 1 - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta))
					== MemoryBuffer::pagesize)
			// if is the second Chunk
			{
				reader = begin;
				MetaData* metaData = (MetaData*) reader;
				reader = reader + metaData->usedSpace;
				--reader;
				return skipBackward(reader);
			}
			reader = begin - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize * ((reader - begin + sizeof(ChunkManagerMeta)) / MemoryBuffer::pagesize - 1);
			MetaData* metaData = (MetaData*) reader;
			reader = reader + metaData->usedSpace;
			--reader;
			return skipBackward(reader);
		} else if (reader <= begin + sizeof(MetaData)) {
			return begin - 1;
		} else {
			//if is not the begin of one Chunk
			return skipBackward(reader);
		}
	}
	if (type == 2) {
		if ((reader - begin - sizeof(MetaData)) % MemoryBuffer::pagesize == 0 || (reader + 1 - begin - sizeof(MetaData)) % MemoryBuffer::pagesize == 0) {
			reader = begin + MemoryBuffer::pagesize * ((reader - begin) / MemoryBuffer::pagesize - 1);
			MetaData* metaData = (MetaData*) reader;
			reader = reader + metaData->usedSpace;
			--reader;
			return skipBackward(reader);
		} else if (reader <= begin + sizeof(MetaData)) {
			return begin - 1;
		} else {
			//if is not the begin of one Chunk
			return skipBackward(reader);
		}
	}*/
}
