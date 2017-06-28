/*
 * PartitionMaster.cpp
 *
 *  Created on: 2013-8-19
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitRepository.h"
#include "EntityIDBuffer.h"
#include "TripleBitQueryGraph.h"
#include "util/BufferManager.h"
#include "util/PartitionBufferManager.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/TasksQueueChunk.h"
#include "comm/subTaskPackage.h"
#include "TempBuffer.h"
#include "MMapBuffer.h"
#include "PartitionMaster.h"
#include "ThreadPool.h"
#include "TempMMapBuffer.h"

//#define MYDEBUG
//#define MYTESTDEBUG
//#define TESTDEBUG
#define TESTINSERT
//#define TTDEBUG

PartitionMaster::PartitionMaster(TripleBitRepository*& repo, const ID parID) {
	tripleBitRepo = repo;
	bitmap = repo->getBitmapBuffer();

	workerNum = repo->getWorkerNum();
	partitionNum = repo->getPartitionNum();
	vector<TasksQueueWP*> tasksQueueWP = repo->getTasksQueueWP();
	vector<ResultBuffer*> resultWP = repo->getResultWP();

	partitionID = parID;
	partitionChunkManager[0] = bitmap->getChunkManager(partitionID, ORDERBYS);
	partitionChunkManager[1] = bitmap->getChunkManager(partitionID, ORDERBYO);

	tasksQueue = tasksQueueWP[partitionID - 1];
	for (int workerID = 1; workerID <= workerNum; ++workerID) {
		resultBuffer[workerID] = resultWP[(workerID - 1) * partitionNum
				+ partitionID - 1];
	}

	unsigned chunkSizeAll = 0;
	for (int type = 0; type < 2; type++) {
		const uchar* startPtr = partitionChunkManager[type]->getStartPtr();
		xChunkNumber[type] = partitionChunkManager[type]->getChunkNumber();
		chunkSizeAll += xChunkNumber[type];
		ID chunkID = 0;
		xChunkQueue[type][0] = new TasksQueueChunk(startPtr, chunkID, type);
		xChunkTempBuffer[type][0] = new TempBuffer;
		for (chunkID = 1; chunkID < xChunkNumber[type]; chunkID++) {
			xChunkQueue[type][chunkID] = new TasksQueueChunk(
					startPtr + chunkID * MemoryBuffer::pagesize
							- sizeof(ChunkManagerMeta), chunkID, type);
			xChunkTempBuffer[type][chunkID] = new TempBuffer;
		}
	}

	partitionBufferManager = new PartitionBufferManager(chunkSizeAll);
}

void PartitionMaster::endupdate() {
	for (int soType = 0; soType < 2; ++soType) {
		const uchar *startPtr = partitionChunkManager[soType]->getStartPtr();
		ID chunkID = 0;
		combineTempBufferToSource(xChunkTempBuffer[soType][chunkID], startPtr,
				chunkID, soType);
		for (chunkID = 1; chunkID < xChunkNumber[soType]; ++chunkID) {
			if (!xChunkTempBuffer[soType][chunkID]->isEmpty()) {
				combineTempBufferToSource(xChunkTempBuffer[soType][chunkID],
						startPtr - sizeof(ChunkManagerMeta)
								+ chunkID * MemoryBuffer::pagesize, chunkID,
						soType);
			}
		}
	}
}

PartitionMaster::~PartitionMaster() {
	for (int type = 0; type < 2; type++) {
		for (unsigned chunkID = 0; chunkID < xChunkNumber[type]; chunkID++) {
			if (xChunkQueue[type][chunkID]) {
				delete xChunkQueue[type][chunkID];
				xChunkQueue[type][chunkID] = NULL;
			}
			if (xChunkTempBuffer[type][chunkID]) {
				delete xChunkTempBuffer[type][chunkID];
				xChunkTempBuffer[type][chunkID] = NULL;
			}
		}
		for (unsigned chunkID = 0; chunkID < xyChunkNumber[type]; chunkID++) {
			if (xyChunkQueue[type][chunkID]) {
				delete xyChunkQueue[type][chunkID];
				xChunkQueue[type][chunkID] = NULL;
			}
			if (xyChunkTempBuffer[type][chunkID]) {
				delete xyChunkTempBuffer[type][chunkID];
				xChunkTempBuffer[type][chunkID] = NULL;
			}
		}
	}

	if (partitionBufferManager) {
		delete partitionBufferManager;
		partitionBufferManager = NULL;
	}
}

void PartitionMaster::Work() {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif
	while (1) {
		SubTrans* subTransaction = tasksQueue->Dequeue();

		switch (subTransaction->operationType) {
		case TripleBitQueryGraph::QUERY:
			executeQuery(subTransaction);
			delete subTransaction;
			break;
		case TripleBitQueryGraph::INSERT_DATA:
			executeInsertData(subTransaction);
			delete subTransaction;
			break;
		case TripleBitQueryGraph::DELETE_DATA:
			executeDeleteData(subTransaction);
			delete subTransaction;
			break;
		case TripleBitQueryGraph::DELETE_CLAUSE:
			executeDeleteClause(subTransaction);
			delete subTransaction;
			break;
		case TripleBitQueryGraph::UPDATE:
			SubTrans *subTransaction2 = tasksQueue->Dequeue();
			executeUpdate(subTransaction, subTransaction2);
			delete subTransaction;
			delete subTransaction2;
			break;
		}
	}
}

void PartitionMaster::executeQuery(SubTrans *subTransaction) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	ID minID = subTransaction->minID;
	ID maxID = subTransaction->maxID;
	TripleNode *triple = &(subTransaction->triple);
	size_t chunkCount, xChunkCount, xyChunkCount;
	size_t xChunkIDMin, xChunkIDMax, xyChunkIDMin, xyChunkIDMax;
	int soType, xyType;
	xChunkIDMin = xChunkIDMax = xyChunkIDMin = xyChunkIDMax = 0;
	chunkCount = xChunkCount = xyChunkCount = 0;

	switch (triple->scanOperation) {
	case TripleNode::FINDOSBYP: {
		soType = ORDERBYO;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				minID, minID + 1, xChunkIDMin) && minID <= maxID)
			minID++;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				maxID, UINT_MAX, xChunkIDMax) && maxID >= minID)
			maxID--;
		if (minID <= maxID)
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;
		break;
	}
	case TripleNode::FINDSOBYP: {
		soType = ORDERBYS;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				minID, minID + 1, xChunkIDMin) && minID <= maxID)
			minID++;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				maxID, UINT_MAX, xChunkIDMax) && maxID >= minID)
			maxID--;
		if (minID <= maxID)
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;

		break;
	}
	case TripleNode::FINDSBYPO: {
		soType = ORDERBYO;

		if (partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				triple->object, triple->object + 1, xChunkIDMin)) {
			if (partitionChunkManager[soType]->getChunkIndex()->searchChunk(
					triple->object, maxID, xChunkIDMax)) {
				assert(xChunkIDMax >= xChunkIDMin);
				xChunkCount = xChunkIDMax - xChunkIDMin + 1;
			}
		} else
			xChunkCount = 0;
		break;
	}
	case TripleNode::FINDOBYSP: {
		soType = ORDERBYS;
		if (partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				triple->subjectID, triple->subjectID + 1, xChunkIDMin)) {
			xChunkIDMax =
					partitionChunkManager[soType]->getChunkIndex()->searchChunk(
							triple->subjectID, maxID);
			assert(xChunkIDMax >= xChunkIDMin);
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;
		} else
			xChunkCount = 0;
		break;
	}
	case TripleNode::FINDSBYP: {
		soType = ORDERBYS;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				minID, minID + 1, xChunkIDMin) && minID <= maxID)
			minID++;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				maxID, UINT_MAX, xChunkIDMax) && minID <= maxID)
			maxID--;
		if (xChunkIDMax >= xChunkIDMin)
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;

		break;
	}
	case TripleNode::FINDOBYP: {
		soType = 1;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				minID, minID + 1, xChunkIDMin) && minID <= maxID)
			minID++;
		while (!partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				maxID, UINT_MAX, xChunkIDMax) && minID <= maxID)
			maxID--;
		if (xChunkIDMax >= xChunkIDMin)
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;

		break;
	}
	}

	if (xChunkCount + xyChunkCount == 0) {
		//EmptyResult
		cout << "Empty Result" << endl;
		return;
	}

	ID sourceWorkerID = subTransaction->sourceWorkerID;
	chunkCount = xChunkCount + xyChunkCount;
	shared_ptr<subTaskPackage> taskPackage(
			new subTaskPackage(chunkCount, subTransaction->operationType,
					sourceWorkerID, subTransaction->minID,
					subTransaction->maxID, 0, 0, partitionBufferManager));

#ifdef RESULT_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif

	if (xChunkCount != 0) {
		for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax;
				offsetID++) {
			ChunkTask *chunkTask = new ChunkTask(subTransaction->operationType,
					triple->subjectID, triple->object, triple->objType,
					triple->scanOperation, taskPackage,
					subTransaction->indexForTT);
			taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
		}
	}
	if (xyChunkCount != 0) {
		for (size_t offsetID = xyChunkIDMin; offsetID <= xyChunkIDMax;
				offsetID++) {
			ChunkTask *chunkTask = new ChunkTask(subTransaction->operationType,
					triple->subjectID, triple->object, triple->objType,
					triple->scanOperation, taskPackage,
					subTransaction->indexForTT);
			taskEnQueue(chunkTask, xyChunkQueue[soType][offsetID]);
		}
	}

#ifdef RESULT_TIME
	gettimeofday(&end, NULL);
	cerr << "taskEnQueue time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << " s" << endl;
#endif
}

void PartitionMaster::executeInsertData(SubTrans* subTransaction) {
#ifdef TTDEBUG
	cout << __FUNCTION__ << endl;
#endif
	ID subjectID = subTransaction->triple.subjectID;
	double object = subTransaction->triple.object;
	char objType = subTransaction->triple.objType;
	size_t chunkID;
	shared_ptr<subTaskPackage> taskPackage(new subTaskPackage);

	chunkID = partitionChunkManager[ORDERBYS]->getChunkIndex()->searchChunk(
			subjectID, object);
	ChunkTask *chunkTask1 = new ChunkTask(subTransaction->operationType,
			subjectID, object, objType, subTransaction->triple.scanOperation,
			taskPackage, subTransaction->indexForTT);
	taskEnQueue(chunkTask1, xChunkQueue[ORDERBYS][chunkID]);

	chunkID = partitionChunkManager[ORDERBYO]->getChunkIndex()->searchChunk(
			object, subjectID);
	ChunkTask *chunkTask2 = new ChunkTask(subTransaction->operationType,
			subjectID, object, objType, subTransaction->triple.scanOperation,
			taskPackage, subTransaction->indexForTT);
	taskEnQueue(chunkTask2, xChunkQueue[ORDERBYO][chunkID]);

//	subTransaction->indexForTT->completeOneTriple();
}

void PartitionMaster::executeDeleteData(SubTrans* subTransaction) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << endl;
#endif
	executeInsertData(subTransaction);
}

void PartitionMaster::executeDeleteClause(SubTrans* subTransaction) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << endl;
#endif
	ID subjectID = subTransaction->triple.subjectID;
	double object = subTransaction->triple.object;
	char objType = subTransaction->triple.objType;
	bool soType;
	size_t xChunkIDMin, xChunkIDMax;
	size_t chunkCount, xChunkCount;
	xChunkIDMin = xChunkIDMax = 0;
	chunkCount = xChunkCount = 0;

	if (subTransaction->triple.constSubject) {
		soType = ORDERBYS;
		if (partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				subjectID, subjectID + 1, xChunkIDMin)) {
			xChunkIDMax =
					partitionChunkManager[soType]->getChunkIndex()->searchChunk(
							subjectID, UINT_MAX);
			assert(xChunkIDMax >= xChunkIDMin);
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;
		} else {
			return;
		}

		chunkCount = xChunkCount;
		shared_ptr<subTaskPackage> taskPackage(
				new subTaskPackage(chunkCount, subTransaction->operationType, 0,
						0, 0, subjectID, 0, partitionBufferManager));
		if (xChunkCount != 0) {
			for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransaction->operationType, subjectID, object,
						objType, subTransaction->triple.scanOperation,
						taskPackage, subTransaction->indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
			}
		}
	} else {
		soType = ORDERBYO;
		if (partitionChunkManager[soType]->getChunkIndex()->searchChunk(object,
				object + 1, xChunkIDMin)) {
			xChunkIDMax =
					partitionChunkManager[soType]->getChunkIndex()->searchChunk(
							object, UINT_MAX);
			assert(xChunkIDMax >= xChunkIDMin);
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;
		} else {
			return;
		}

		chunkCount = xChunkCount;
		shared_ptr<subTaskPackage> taskPackage(
				new subTaskPackage(chunkCount, subTransaction->operationType, 0,
						0, 0, object, 0, partitionBufferManager));
		if (xChunkCount != 0) {
			for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransaction->operationType, subjectID, object,
						objType, subTransaction->triple.scanOperation,
						taskPackage, subTransaction->indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
			}
		}
	}
}

void PartitionMaster::executeUpdate(SubTrans *subTransfirst,
		SubTrans *subTranssecond) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << endl;
#endif
	ID subjectID = subTransfirst->triple.subjectID;
	double object = subTransfirst->triple.object;
	char objType = subTransfirst->triple.objType;
	ID subUpdate = subTranssecond->triple.subjectID;
	double obUpdate = subTranssecond->triple.object;
	char objTypeUpdate = subTranssecond->triple.objType;
	int soType;
	size_t xChunkIDMin, xChunkIDMax;
	size_t chunkCount, xChunkCount;
	xChunkIDMin = xChunkIDMax = 0;
	chunkCount = xChunkCount = 0;

	if (subTransfirst->triple.constSubject) {
		soType = ORDERBYS;
		if (partitionChunkManager[soType]->getChunkIndex()->searchChunk(
				subjectID, subjectID + 1, xChunkIDMin)) {
			xChunkIDMax =
					partitionChunkManager[soType]->getChunkIndex()->searchChunk(
							subjectID, UINT_MAX);
			assert(xChunkIDMax >= xChunkIDMin);
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;
		} else {
			return;
		}
		chunkCount = xChunkCount;
		shared_ptr<subTaskPackage> taskPackage(
				new subTaskPackage(chunkCount, subTransfirst->operationType, 0,
						0, 0, subjectID, subUpdate, partitionBufferManager));
		if (xChunkCount != 0) {
			for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransfirst->operationType, subjectID, object,
						objType, subTransfirst->triple.scanOperation,
						taskPackage, subTransfirst->indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
			}
		}
	} else {
		soType = ORDERBYO;
		if (partitionChunkManager[soType]->getChunkIndex()->searchChunk(object,
				object + 1, xChunkIDMin)) {
			xChunkIDMax =
					partitionChunkManager[soType]->getChunkIndex()->searchChunk(
							object, UINT_MAX);
			assert(xChunkIDMax >= xChunkIDMin);
			xChunkCount = xChunkIDMax - xChunkIDMin + 1;
		} else {
			return;
		}

		chunkCount = xChunkCount;
		shared_ptr<subTaskPackage> taskPackage(
				new subTaskPackage(chunkCount, subTransfirst->operationType, 0,
						0, 0, object, obUpdate, partitionBufferManager));
		if (xChunkCount != 0) {
			for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransfirst->operationType, subjectID, object,
						objType, subTransfirst->triple.scanOperation,
						taskPackage, subTransfirst->indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
			}
		}
	}
}

void PrintChunkTaskPart(ChunkTask* chunkTask) {
	cout << "opType:" << chunkTask->operationType << " subject:"
			<< chunkTask->Triple.subjectID << " object:"
			<< chunkTask->Triple.object << " operation:"
			<< chunkTask->Triple.operation << endl;
}

void PartitionMaster::taskEnQueue(ChunkTask *chunkTask,
		TasksQueueChunk *tasksQueue) {
	if (tasksQueue->isEmpty()) {
		tasksQueue->EnQueue(chunkTask);
		ThreadPool::getChunkPool().addTask(
				boost::bind(&PartitionMaster::handleTasksQueueChunk, this,
						tasksQueue));
	} else {
		tasksQueue->EnQueue(chunkTask);
	}
}

void PartitionMaster::handleTasksQueueChunk(TasksQueueChunk* tasksQueue) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	ChunkTask* chunkTask = NULL;
	ID chunkID = tasksQueue->getChunkID();
	bool soType = tasksQueue->getSOType();
	const uchar* chunkBegin = tasksQueue->getChunkBegin();

	while ((chunkTask = tasksQueue->Dequeue()) != NULL) {
		switch (chunkTask->operationType) {
		case TripleBitQueryGraph::QUERY:
			executeChunkTaskQuery(chunkTask, chunkID, chunkBegin);
			break;
		case TripleBitQueryGraph::INSERT_DATA:
			executeChunkTaskInsertData(chunkTask, chunkID, chunkBegin, soType);
			break;
		case TripleBitQueryGraph::DELETE_DATA:
			executeChunkTaskDeleteData(chunkTask, chunkID, chunkBegin, soType);
			break;
		case TripleBitQueryGraph::DELETE_CLAUSE:
			executeChunkTaskDeleteClause(chunkTask, chunkID, chunkBegin, soType);
			break;
		case TripleBitQueryGraph::UPDATE:
			executeChunkTaskUpdate(chunkTask, chunkID, chunkBegin, soType);
			break;
		}
	}
//	tasksQueue->setOutOfThreadPool();
}

void PartitionMaster::executeChunkTaskInsertData(ChunkTask *chunkTask,
		const ID chunkID, const uchar *startPtr, const bool soType) {
//	chunkTask->indexForTT->completeOneTriple();
	xChunkTempBuffer[soType][chunkID]->insertTriple(chunkTask->Triple.subjectID,
			chunkTask->Triple.object, chunkTask->Triple.objType);
	if (xChunkTempBuffer[soType][chunkID]->isFull()) {
		//combine the data in tempbuffer into the source data
		combineTempBufferToSource(xChunkTempBuffer[soType][chunkID], startPtr,
				chunkID, soType);
	}

	chunkTask->indexForTT->completeOneTriple();
}

void PartitionMaster::readIDInTempPage(const uchar *&currentPtrTemp,
		const uchar *&endPtrTemp, const uchar *&startPtrTemp, char *&tempPage,
		char *&tempPage2, bool &theOtherPageEmpty, bool &isInTempPage) {
	if (currentPtrTemp >= endPtrTemp) {
		//the Ptr has reach the end of the page
		if (theOtherPageEmpty) {
			//TempPage is ahead of Chunk
			MetaData *metaData = (MetaData*) startPtrTemp;
			if (metaData->NextPageNo) {
				//TempPage still have followed Page
				size_t pageNo = metaData->NextPageNo;
				memcpy(tempPage,
						TempMMapBuffer::getInstance().getAddress()
								+ pageNo * MemoryBuffer::pagesize,
						MemoryBuffer::pagesize);
				isInTempPage = true;

				startPtrTemp = reinterpret_cast<uchar*>(tempPage);
				currentPtrTemp = startPtrTemp + sizeof(MetaData);
				endPtrTemp = startPtrTemp
						+ ((MetaData*) startPtrTemp)->usedSpace;
			}
		} else {
			//Chunk ahead of TempPage, but Chunk is ahead one Page most,that is Chunk is on the next page of the TempPage
			if (isInTempPage) {
				startPtrTemp = reinterpret_cast<uchar*>(tempPage2);
				isInTempPage = false;
			} else {
				startPtrTemp = reinterpret_cast<uchar*>(tempPage);
				isInTempPage = true;
			}
			currentPtrTemp = startPtrTemp + sizeof(MetaData);
			endPtrTemp = startPtrTemp + ((MetaData*) startPtrTemp)->usedSpace;
			theOtherPageEmpty = true;
		}
	}
}

void PartitionMaster::handleEndofChunk(const uchar *startPtr,
		uchar *&chunkBegin, uchar*&startPtrChunk, uchar *&currentPtrChunk,
		uchar *&endPtrChunk, const uchar *&startPtrTemp, char *&tempPage,
		char *&tempPage2, bool &isInTempPage, bool &theOtherPageEmpty,
		double min, double max, bool soType, const ID chunkID) {
	assert(currentPtrChunk <= endPtrChunk);
	MetaData *metaData = NULL;
	if (chunkID == 0 && chunkBegin == startPtr - sizeof(ChunkManagerMeta)) {
		metaData = (MetaData*) startPtr;
		metaData->usedSpace = currentPtrChunk - startPtr;
	} else {
		metaData = (MetaData*) chunkBegin;
		metaData->usedSpace = currentPtrChunk - chunkBegin;
	}
	metaData->min = min;
	metaData->max = max;

	if (metaData->NextPageNo) {
		MetaData *metaDataTemp = (MetaData*) startPtrTemp;
		size_t pageNo = metaData->NextPageNo;
		if (metaDataTemp->NextPageNo <= pageNo) {
			if (isInTempPage)
				memcpy(tempPage2,
						TempMMapBuffer::getInstance().getAddress()
								+ pageNo * MemoryBuffer::pagesize,
						MemoryBuffer::pagesize);
			else
				memcpy(tempPage,
						TempMMapBuffer::getInstance().getAddress()
								+ pageNo * MemoryBuffer::pagesize,
						MemoryBuffer::pagesize);
			theOtherPageEmpty = false;
		}
		chunkBegin =
				reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
						+ pageNo * MemoryBuffer::pagesize;
		metaData = (MetaData*) chunkBegin;
	} else {
		//get a new Page from TempMMapBuffer
		size_t pageNo = 0;
		chunkBegin =
				reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getPage(
						pageNo));

		metaData->NextPageNo = pageNo;

		metaData = (MetaData*) chunkBegin;
		metaData->NextPageNo = 0;
	}
	startPtrChunk = currentPtrChunk = chunkBegin + sizeof(MetaData);
	endPtrChunk = chunkBegin + MemoryBuffer::pagesize;
	assert(currentPtrChunk <= endPtrChunk);
}

void PartitionMaster::combineTempBufferToSource(TempBuffer *buffer,
		const uchar *startPtr, const ID chunkID, const bool soType) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	assert(buffer != NULL);

	buffer->sort(soType);

	buffer->uniqe();

	if (buffer->isEmpty())
		return;

	char *tempPage = (char*) malloc(MemoryBuffer::pagesize);
	char *tempPage2 = (char*) malloc(MemoryBuffer::pagesize);

	if (tempPage == NULL || tempPage2 == NULL) {
		cout << "malloc a tempPage error" << endl;
		free(tempPage);
		free(tempPage2);
		return;
	}
	memset(tempPage, 0, sizeof(char));
	memset(tempPage2, 0, sizeof(char));

	uchar *currentPtrChunk, *endPtrChunk, *chunkBegin, *startPtrChunk;
	const uchar *lastPtrTemp, *currentPtrTemp, *endPtrTemp, *startPtrTemp;
	bool isInTempPage = true, theOtherPageEmpty = true;

	if (chunkID == 0) {
		chunkBegin = const_cast<uchar*>(startPtr) - sizeof(ChunkManagerMeta);
		memcpy(tempPage, chunkBegin, MemoryBuffer::pagesize);
		startPtrChunk = currentPtrChunk = chunkBegin + sizeof(ChunkManagerMeta)
				+ sizeof(MetaData);
		lastPtrTemp = currentPtrTemp = reinterpret_cast<uchar*>(tempPage)
				+ sizeof(ChunkManagerMeta) + sizeof(MetaData);
	} else {
		chunkBegin = const_cast<uchar*>(startPtr);
		memcpy(tempPage, chunkBegin, MemoryBuffer::pagesize);
		startPtrChunk = currentPtrChunk = chunkBegin + sizeof(MetaData);
		lastPtrTemp = currentPtrTemp = reinterpret_cast<uchar*>(tempPage)
				+ sizeof(MetaData);
	}
	endPtrChunk = chunkBegin + MemoryBuffer::pagesize;
	startPtrTemp = lastPtrTemp - sizeof(MetaData);
	endPtrTemp = startPtrTemp + ((MetaData*) startPtrTemp)->usedSpace;

	SOCouple* chunkTriple, *tempTriple;
	SOCouple *lastTempBuffer, *currentTempBuffer, *endTempBuffer;
	SOCouple *start = buffer->getBuffer(), *end = buffer->getEnd();
	lastTempBuffer = currentTempBuffer = start;
	endTempBuffer = end;

	chunkTriple = (SOCouple*) malloc(sizeof(SOCouple));
	if(chunkTriple == NULL){
		cout << "malloc a SOCouple error" << endl;
		free(chunkTriple);
		return;
	}
	memset(chunkTriple, 0, sizeof(char));
	*tempTriple = *currentTempBuffer;
	double max = DBL_MIN, min = DBL_MIN;

	if (currentPtrTemp >= endPtrTemp) {
		chunkTriple->subjectID = 0;
		chunkTriple->object = 0;
		chunkTriple->objType = NONE;
	} else {
		currentPtrTemp = partitionChunkManager[soType]->readXY(currentPtrTemp,
				chunkTriple->subjectID, chunkTriple->object,
				chunkTriple->objType);
	}

	while (lastPtrTemp < endPtrTemp && lastTempBuffer < endTempBuffer) {
		//the Ptr not reach the end
		if (chunkTriple->subjectID == 0
				|| (chunkTriple->subjectID == tempTriple->subjectID
						&& chunkTriple->object == tempTriple->object
						&& chunkTriple->objType == tempTriple->objType)) {
			//the data is 0 or the data in chunk and tempbuffer are same,so must dismiss it
			readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp, tempPage,
					tempPage2, theOtherPageEmpty, isInTempPage);
			lastPtrTemp = currentPtrTemp;
			if (currentPtrTemp >= endPtrTemp) {
				chunkTriple->subjectID = 0;
				chunkTriple->object = 0;
				chunkTriple->objType = NONE;
			} else {
				currentPtrTemp = partitionChunkManager[soType]->readXY(
						currentPtrTemp, chunkTriple->subjectID,
						chunkTriple->object, chunkTriple->objType);
			}
		} else {
			if (chunkTriple->subjectID < tempTriple->subjectID
					|| (chunkTriple->subjectID == tempTriple->subjectID
							&& chunkTriple->object < tempTriple->object)
					|| (chunkTriple->subjectID == tempTriple->subjectID
							&& chunkTriple->object == tempTriple->object
							&& chunkTriple->objType != tempTriple->objType)) {
				uint len = currentPtrTemp - lastPtrTemp;
				if (currentPtrChunk + len > endPtrChunk) {
					handleEndofChunk(startPtr, chunkBegin, startPtrChunk,
							currentPtrChunk, endPtrChunk, startPtrTemp,
							tempPage, tempPage2, isInTempPage,
							theOtherPageEmpty, min, max, soType, chunkID);
				}
				if (currentPtrChunk == startPtrChunk) {
					min = getChunkMinOrMax(chunkTriple, soType);
				}
				memcpy(currentPtrChunk, lastPtrTemp, len);
				max = getChunkMinOrMax(chunkTriple, soType);

				currentPtrChunk += len;
//					assert(currentPtrChunk <= endPtrChunk);

//continue read data from tempPage
				readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp,
						tempPage, tempPage2, theOtherPageEmpty, isInTempPage);
				lastPtrTemp = currentPtrTemp;
				if (currentPtrTemp >= endPtrTemp) {
					chunkTriple->subjectID = 0;
					chunkTriple->object = 0;
					chunkTriple->objType = NONE;
				} else {
					currentPtrTemp = partitionChunkManager[soType]->readXY(
							currentPtrTemp, chunkTriple->subjectID,
							chunkTriple->object, chunkTriple->objType);
				}
			} else {
				//insert data read from tempbuffer
				uint len = sizeof(ID) + sizeof(char)
						+ Chunk::getLen(tempTriple->objType);
				if (currentPtrChunk + len > endPtrChunk) {
					handleEndofChunk(startPtr, chunkBegin, startPtrChunk,
							currentPtrChunk, endPtrChunk, startPtrTemp,
							tempPage, tempPage2, isInTempPage,
							theOtherPageEmpty, min, max, soType, chunkID);
				}
				if (currentPtrChunk == startPtrChunk) {
					min = getChunkMinOrMax(tempTriple, soType);
				}
				partitionChunkManager[soType]->writeXY(currentPtrChunk,
						tempTriple->subjectID, tempTriple->object,
						tempTriple->objType);
				max = getChunkMinOrMax(tempTriple, soType);
//					assert(currentPtrChunk <= endPtrChunk);

				lastTempBuffer = currentTempBuffer;
				currentTempBuffer++;
				if (currentTempBuffer < endTempBuffer) {
					*tempTriple = *currentTempBuffer;
				}
			}
		}
	}

	while (lastPtrTemp < endPtrTemp) {
		if (chunkTriple->subjectID == 0) {
			readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp, tempPage,
					tempPage2, theOtherPageEmpty, isInTempPage);
			lastPtrTemp = currentPtrTemp;
			if (currentPtrTemp >= endPtrTemp) {
				chunkTriple->subjectID = 0;
				chunkTriple->object = 0;
				chunkTriple->objType = NONE;
			} else {
				currentPtrTemp = partitionChunkManager[soType]->readXY(
						currentPtrTemp, chunkTriple->subjectID,
						chunkTriple->object, chunkTriple->objType);
			}
		} else {
			unsigned len = currentPtrTemp - lastPtrTemp;
			if (currentPtrChunk + len > endPtrChunk) {
				handleEndofChunk(startPtr, chunkBegin, startPtrChunk,
						currentPtrChunk, endPtrChunk, startPtrTemp, tempPage,
						tempPage2, isInTempPage, theOtherPageEmpty, min, max,
						soType, chunkID);
			}
			if (currentPtrChunk == startPtrChunk) {
				min = getChunkMinOrMax(chunkTriple, soType);
			}
			memcpy(currentPtrChunk, lastPtrTemp, len);
			max = getChunkMinOrMax(chunkTriple, soType);
			currentPtrChunk += len;
//				assert(currentPtrChunk <= endPtrChunk);

//continue read data from tempPage
			readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp, tempPage,
					tempPage2, theOtherPageEmpty, isInTempPage);
			lastPtrTemp = currentPtrTemp;
			if (currentPtrTemp >= endPtrTemp) {
				chunkTriple->subjectID = 0;
				chunkTriple->object = 0;
				chunkTriple->objType = NONE;
			} else {
				currentPtrTemp = partitionChunkManager[soType]->readXY(
						currentPtrTemp, chunkTriple->subjectID,
						chunkTriple->object, chunkTriple->objType);
			}
		}
	}

	while (lastTempBuffer < endTempBuffer) {
		uint len = sizeof(ID) + sizeof(char)
				+ Chunk::getLen(tempTriple->objType);

		if (currentPtrChunk + len > endPtrChunk) {
			handleEndofChunk(startPtr, chunkBegin, startPtrChunk,
					currentPtrChunk, endPtrChunk, startPtrTemp, tempPage,
					tempPage2, isInTempPage, theOtherPageEmpty, min, max,
					soType, chunkID);
		}
		if (currentPtrChunk == startPtrChunk) {
			min = getChunkMinOrMax(tempTriple, soType);
		}
		partitionChunkManager[soType]->writeXY(currentPtrChunk,
				tempTriple->subjectID, tempTriple->object, tempTriple->objType);
		max = getChunkMinOrMax(tempTriple, soType);
//			assert(currentPtrChunk <= endPtrChunk);

		lastTempBuffer = currentTempBuffer;
		currentTempBuffer++;
		if (currentTempBuffer < endTempBuffer) {
			*tempTriple = *currentTempBuffer;
		}
	}

	if (chunkBegin == startPtr - sizeof(ChunkManagerMeta)) {
		MetaData *metaData = (MetaData*) startPtr;
		const uchar* reader = startPtr + sizeof(MetaData);
		metaData->min = min;
		metaData->max = max;
		metaData->usedSpace = currentPtrChunk - startPtr;
	} else {
		MetaData *metaData = (MetaData*) chunkBegin;
		metaData->min = min;
		metaData->max = max;
		metaData->usedSpace = currentPtrChunk - chunkBegin;
	}

	partitionChunkManager[soType]->getChunkIndex()->updateChunkMetaData(
			chunkID);

	free(chunkTriple);
	chunkTriple = NULL;
	free(tempPage);
	tempPage = NULL;
	free(tempPage2);
	tempPage2 = NULL;

	buffer->clear();
}

void PartitionMaster::executeChunkTaskDeleteData(ChunkTask *chunkTask,
		const ID chunkID, const uchar* startPtr, const bool soType) {
	ID subjectID = chunkTask->Triple.subjectID;
	ID tempSubjectID;
	double object = chunkTask->Triple.object;
	double tempObject;
	char objType = chunkTask->Triple.objType;
	char tempObjType;

	const uchar *reader, *limit, *chunkBegin = startPtr;
	uchar *temp;

	MetaData *metaData = (MetaData*) chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	if (soType == ORDERBYS) {
		while (reader < limit) {
			temp = const_cast<uchar*>(reader);
			reader = partitionChunkManager[soType]->readXY(reader,
					tempSubjectID, tempObject, tempObjType);
			if (tempSubjectID < subjectID
					|| (tempSubjectID == subjectID && tempObject < object)
					|| (tempSubjectID == subjectID && tempObject == object
							&& tempObjType != objType))
				continue;
			else if (tempSubjectID == subjectID && tempObject == object
					&& tempObjType == objType) {
				temp = partitionChunkManager[soType]->deleteTriple(temp,
						objType);
				return;
			} else {
				return;
			}
		}
		while (metaData->NextPageNo) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ MemoryBuffer::pagesize * metaData->NextPageNo;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				temp = const_cast<uchar*>(reader);
				reader = partitionChunkManager[soType]->readXY(reader,
						tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID < subjectID
						|| (tempSubjectID == subjectID && tempObject < object)
						|| (tempSubjectID == subjectID && tempObject == object
								&& tempObjType != objType))
					continue;
				else if (tempSubjectID == subjectID && tempObject == object
						&& tempObjType == objType) {
					temp = partitionChunkManager[soType]->deleteTriple(temp,
							objType);
					return;
				} else {
					return;
				}
			}
		}
	} else if (soType == ORDERBYO) {
		while (reader < limit) {
			temp = const_cast<uchar*>(reader);
			reader = partitionChunkManager[soType]->readXY(reader,
					tempSubjectID, tempObject, tempObjType);
			if (tempObject < object
					|| (tempObject == object && tempObjType != objType)
					|| (tempObject == object && tempObjType == objType
							&& tempSubjectID < subjectID))
				continue;
			else if (tempObject == object && tempObjType == objType
					&& tempSubjectID == subjectID) {
				temp = partitionChunkManager[soType]->deleteTriple(temp,
						objType);
				return;
			} else {
				return;
			}
		}
		while (metaData->NextPageNo) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ MemoryBuffer::pagesize * metaData->NextPageNo;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				temp = const_cast<uchar*>(reader);
				reader = partitionChunkManager[soType]->readXY(reader,
						tempSubjectID, tempObject, tempObjType);
				if (tempObject < object
						|| (tempObject == object && tempObjType != objType)
						|| (tempObject == object && tempObjType == objType
								&& tempSubjectID < subjectID))
					continue;
				else if (tempObject == object && tempObjType == objType
						&& tempSubjectID == subjectID) {
					temp = partitionChunkManager[soType]->deleteTriple(temp,
							objType);
					return;
				} else {
					return;
				}
			}
		}
	}
}

void PartitionMaster::deleteDataForDeleteClause(EntityIDBuffer *buffer,
		const ID deleteID, const bool soType) {
/*	size_t size = buffer->getSize();
	ID *retBuffer = buffer->getBuffer();
	size_t index;
	int chunkID;
	shared_ptr<subTaskPackage> taskPackage(new subTaskPackage);
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_DATA;
	TripleNode::Op scanType = TripleNode::NOOP;

	for (index = 0; index < size; ++index)
		if (retBuffer[index] > deleteID)
			break;
	if (soType == 0) {
		//deleteID -->subject
		int deleteSOType = 1;
		for (size_t i = 0; i < index; ++i) {
			//object < subject == x<y
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(1)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(operationType, deleteID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}
		for (size_t i = index; i < size; ++i) {
			//object >subject == x>y
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(2)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(operationType, deleteID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}
	} else if (soType == 1) {
		//deleteID -->object
		int deleteSOType = 0;
		for (size_t i = 0; i < index; ++i) {
			//subject <object== x<y
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(1)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(operationType, retBuffer[i],
					deleteID, scanType, taskPackage, indexForTT);
			xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}
		for (size_t i = index; i < size; ++i) {
			//subject > object == x >y
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(2)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(operationType, retBuffer[i],
					deleteID, scanType, taskPackage, indexForTT);
			xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}
	}*/
}

void PartitionMaster::executeChunkTaskDeleteClause(ChunkTask *chunkTask,
		const ID chunkID, const uchar *startPtr, const bool soType) {

	/*ID deleteXID = 0, deleteXYID = 0;
	if (soType == 0) {
		if (xyType == 1) {
			//sort by S,x < y
			deleteXID = chunkTask->Triple.subject;
		} else if (xyType == 2) {
			//sort by S,x > y
			deleteXYID = chunkTask->Triple.subject;
		}
	} else if (soType == 1) {
		if (xyType == 1) {
			//sort by O, x<y
			deleteXID = chunkTask->Triple.object;
		} else if (xyType == 2) {
			//sort by O, x>y
			deleteXYID = chunkTask->Triple.object;
		}
	}
	EntityIDBuffer *retBuffer = new EntityIDBuffer;
	retBuffer->empty();
	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);
	register ID x, y;
	const uchar *reader, *limit, *chunkBegin = startPtr;
	uchar *temp;

	MetaData *metaData = (MetaData*) chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;
	while (reader < limit) {
		temp = const_cast<uchar*>(reader);
		reader = Chunk::readYId(Chunk::readXId(reader, x), y);
		if (x < deleteXID)
			continue;
		else if (x == deleteXID) {
			retBuffer->insertID(x + y);
			temp = Chunk::deleteYId(Chunk::deleteXId(temp));
		} else
			goto END;
	}
	while (metaData->NextPageNo) {
		chunkBegin =
				reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
						+ metaData->NextPageNo * MemoryBuffer::pagesize;
		metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;
		while (reader < limit) {
			temp = const_cast<uchar*>(reader);
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x < deleteXID)
				continue;
			else if (x == deleteXID) {
				retBuffer->insertID(x + y);
				temp = Chunk::deleteYId(Chunk::deleteXId(temp));
			} else
				goto END;
		}
	}

	END:
//	chunkTask->taskPackage->completeSubTask(chunkID, retBuffer, xyType);
	if (chunkTask->taskPackage->completeSubTask(chunkID, retBuffer)) {
#ifdef MYTESTDEBUG
		cout << "complete all task deleteClause" << endl;
#endif
		EntityIDBuffer *buffer = chunkTask->taskPackage->getTaskResult();
		ID deleteID = chunkTask->taskPackage->deleteID;
		deleteDataForDeleteClause(buffer, deleteID, soType);

		partitionBufferManager->freeBuffer(buffer);
	}
	retBuffer = NULL;*/
}

void PartitionMaster::updateDataForUpdate(EntityIDBuffer *buffer,
		const ID deleteID, const ID updateID, const bool soType) {
	/*size_t size = buffer->getSize();
	ID *retBuffer = buffer->getBuffer();
	size_t indexDelete, indexUpdate;
	int chunkID;
	shared_ptr<subTaskPackage> taskPackage(new subTaskPackage);
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);
	TripleBitQueryGraph::OpType opDelete = TripleBitQueryGraph::DELETE_DATA;
	TripleBitQueryGraph::OpType opInsert = TripleBitQueryGraph::INSERT_DATA;
	TripleNode::Op scanType = TripleNode::NOOP;

	for (indexDelete = 0; indexDelete < size; ++indexDelete)
		if (retBuffer[indexDelete] > deleteID)
			break;
	for (indexUpdate = 0; indexUpdate < size; ++indexUpdate)
		if (retBuffer[indexUpdate] > updateID)
			break;

	int deleteSOType, insertSOType;
	if (soType == ORDERBYS) {
		//deleteID -->subject
		deleteSOType = 1;
		xyType = 1;
		for (size_t i = 0; i < indexDelete; ++i) {
			//object < subject == x<y
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}
		xyType = 2;
		for (size_t i = indexDelete; i < size; ++i) {
			//object > subject == x>y
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}

		for (size_t i = 0; i < indexUpdate; ++i) {
			//subject > object
			insertSOType = 0;
			xyType = 2;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							updateID, retBuffer[i]);
			ChunkTask *chunkTask1 = new ChunkTask(opInsert, updateID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
			insertSOType = 1;
			xyType = 1;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], updateID);
			ChunkTask *chunkTask2 = new ChunkTask(opInsert, updateID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
		}
		for (size_t i = indexUpdate; i < size; ++i) {
			//subject < object
			insertSOType = 0;
			xyType = 1;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							updateID, retBuffer[i]);
			ChunkTask *chunkTask1 = new ChunkTask(opInsert, updateID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
			insertSOType = 1;
			xyType = 2;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], updateID);
			ChunkTask *chunkTask2 = new ChunkTask(opInsert, updateID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
		}
	} else if (soType == 1) {
		//deleteID-->object
		deleteSOType = 0;
		xyType = 1;
		for (size_t i = 0; i < indexDelete; ++i) {
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}
		xyType = 2;
		for (size_t i = indexDelete; i < size; ++i) {
			chunkID =
					partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], deleteID);
			ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID,
					retBuffer[i], scanType, taskPackage, indexForTT);
			xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
		}

		for (size_t i = 0; i < indexUpdate; ++i) {
			//subject < object
			insertSOType = 0, xyType = 1;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], updateID);
			ChunkTask *chunkTask1 = new ChunkTask(opInsert, retBuffer[i],
					updateID, scanType, taskPackage, indexForTT);
			xChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
			insertSOType = 1, xyType = 2;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							updateID, retBuffer[i]);
			ChunkTask *chunkTask2 = new ChunkTask(opInsert, retBuffer[i],
					updateID, scanType, taskPackage, indexForTT);
			xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
		}
		for (size_t i = indexUpdate; i < size; ++i) {
			//subject > object
			insertSOType = 0;
			xyType = 2;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							retBuffer[i], updateID);
			ChunkTask *chunkTask1 = new ChunkTask(opInsert, retBuffer[i],
					updateID, scanType, taskPackage, indexForTT);
			xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
			insertSOType = 1;
			xyType = 1;
			chunkID =
					partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(
							updateID, retBuffer[i]);
			ChunkTask *chunkTask2 = new ChunkTask(opInsert, retBuffer[i],
					updateID, scanType, taskPackage, indexForTT);
			xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
		}
	}*/
}

void PartitionMaster::executeChunkTaskUpdate(ChunkTask *chunkTask,
		const ID chunkID, const uchar* startPtr,
		const bool soType) {
	/*ID deleteXID = 0, deleteXYID = 0;
	if (soType == 0) {
		if (xyType == 1)
			deleteXID = chunkTask->Triple.subject;
		else if (xyType == 2)
			deleteXYID = chunkTask->Triple.subject;
	} else if (soType == 1) {
		if (xyType == 1)
			deleteXID = chunkTask->Triple.object;
		else if (xyType == 2)
			deleteXYID = chunkTask->Triple.object;
	}
	EntityIDBuffer *retBuffer = new EntityIDBuffer;
	retBuffer->empty();
	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);
	register ID x, y;
	const uchar *reader, *limit, *chunkBegin = startPtr;
	uchar *temp;
	if (xyType == 1) {
		//x < y
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;
		while (reader < limit) {
			temp = const_cast<uchar*>(reader);
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x < deleteXID)
				continue;
			else if (x == deleteXID) {
				retBuffer->insertID(x + y);
				temp = Chunk::deleteYId(Chunk::deleteXId(temp));
			} else
				goto END;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				temp = const_cast<uchar*>(reader);
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x < deleteXID)
					continue;
				else if (x == deleteXID) {
					retBuffer->insertID(x + y);
					temp = Chunk::deleteYId(Chunk::deleteXId(temp));
				} else
					goto END;
			}
		}
	} else if (xyType == 2) {
		//x >y
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;
		while (reader < limit) {
			temp = const_cast<uchar*>(reader);
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x + y < deleteXYID)
				continue;
			else if (x + y == deleteXYID) {
				retBuffer->insertID(x);
				temp = Chunk::deleteYId(Chunk::deleteXId(temp));
			} else
				goto END;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				temp = const_cast<uchar*>(reader);
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x + y < deleteXYID)
					continue;
				else if (x + y == deleteXYID) {
					retBuffer->insertID(x);
					temp = Chunk::deleteYId(Chunk::deleteXId(temp));
				} else
					goto END;
			}
		}
	}
	END: if (chunkTask->taskPackage->completeSubTask(chunkID, retBuffer,
			xyType)) {
#ifdef MYTESTDEBUG
		cout << "complete all task update" << endl;
#endif
		EntityIDBuffer *buffer = chunkTask->taskPackage->getTaskResult();
		ID deleteID = chunkTask->taskPackage->deleteID;
		ID updateID = chunkTask->taskPackage->updateID;
		updateDataForUpdate(buffer, deleteID, updateID, soType);

		partitionBufferManager->freeBuffer(buffer);
	}
	retBuffer = NULL;*/
}

void PartitionMaster::executeChunkTaskQuery(ChunkTask *chunkTask,
		const ID chunkID, const uchar* chunkBegin) {
/*#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

//	EntityIDBuffer* retBuffer = partitionBufferManager->getNewBuffer();
	EntityIDBuffer *retBuffer = new EntityIDBuffer;
	retBuffer->empty();

	switch (chunkTask->Triple.operation) {
	case TripleNode::FINDSBYPO:
		findSubjectIDByPredicateAndObject(chunkTask->Triple.object, retBuffer,
				chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID,
				chunkBegin, xyType);
		break;
	case TripleNode::FINDOBYSP:
		findObjectIDByPredicateAndSubject(chunkTask->Triple.subject, retBuffer,
				chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID,
				chunkBegin, xyType);
		break;
	case TripleNode::FINDSBYP:
		findSubjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID,
				chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	case TripleNode::FINDOBYP:
		findObjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID,
				chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	case TripleNode::FINDOSBYP:
		findObjectIDAndSubjectIDByPredicate(retBuffer,
				chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID,
				chunkBegin, xyType);
		break;
	case TripleNode::FINDSOBYP:
		findSubjectIDAndObjectIDByPredicate(retBuffer,
				chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID,
				chunkBegin, xyType);
		break;
	default:
		cout << "unsupport now! executeChunkTaskQuery" << endl;
		break;
	}

	if (chunkTask->taskPackage->completeSubTask(chunkID, retBuffer, xyType)) {
#ifdef MYTESTDEBUG
		cout << "complete all task" << endl;
#endif

#ifdef GET_RESULT_TIME
		struct timeval start, end;
		gettimeofday(&start, NULL);
#endif

//		EntityIDBuffer* buffer = chunkTask->taskPackage->getTaskResult();
		ResultIDBuffer* buffer = new ResultIDBuffer(chunkTask->taskPackage);

		resultBuffer[chunkTask->taskPackage->sourceWorkerID]->EnQueue(buffer);
		delete chunkTask;

#ifdef GET_RESULT_TIME
		gettimeofday(&end, NULL);
		cerr << "getTaskResult time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << " s" << endl;
#endif

#ifdef MYTESTDEBUG
		cout << "PartitionID: " << partitionID << endl;
		printSomething(buffer);
#endif
	}
	retBuffer = NULL;*/
}

void PartitionMaster::findObjectIDByPredicateAndSubject(const ID subjectID,
		EntityIDBuffer *retBuffer, const ID minID, const ID maxID,
		const uchar* startPtr, const int xyType) {
/*#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	if (minID == 0 && maxID == UINT_MAX) {
		findObjectIDByPredicateAndSubject(subjectID, retBuffer, startPtr,
				xyType);
		return;
	}

	register ID x, y;
	const uchar* limit, *reader, *chunkBegin = startPtr;

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	if (xyType == 1) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;

		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x < subjectID)
				continue;
			else if (x == subjectID) {
				if (x + y < minID)
					continue;
				else if (x + y <= maxID)
					retBuffer->insertID(x + y);
				else
					return;
			} else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ MemoryBuffer::pagesize * metaData->NextPageNo;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;

			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x < subjectID)
					continue;
				else if (x == subjectID) {
					if (x + y < minID)
						continue;
					else if (x + y <= maxID)
						retBuffer->insertID(x + y);
					else
						return;
				} else
					return;
			}
		}
	} else if (xyType == 2) {
		MetaData* metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;
		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x + y < subjectID)
				continue;
			else if (x + y == subjectID) {
				if (x < minID)
					continue;
				else if (x <= maxID)
					retBuffer->insertID(x);
				else
					return;
			} else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ MemoryBuffer::pagesize * metaData->NextPageNo;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x + y < subjectID)
					continue;
				else if (x + y == subjectID) {
					if (x < minID)
						continue;
					else if (x <= maxID)
						retBuffer->insertID(x);
					else
						return;
				} else
					return;
			}
		}
	}*/
}

void PartitionMaster::findObjectIDByPredicateAndSubject(const ID subjectID,
		EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
	/*register ID x, y;
	const uchar *reader, *limit, *chunkBegin = startPtr;

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	if (xyType == 1) {
		MetaData* metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;

		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x < subjectID)
				continue;
			else if (x == subjectID)
				retBuffer->insertID(x + y);
			else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ MemoryBuffer::pagesize * metaData->NextPageNo;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;

			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x < subjectID)
					continue;
				else if (x == subjectID)
					retBuffer->insertID(x + y);
				else
					return;
			}
		}
	} else if (xyType == 2) {
		MetaData* metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;

		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x + y < subjectID)
				continue;
			else if (x + y == subjectID)
				retBuffer->insertID(x);
			else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ MemoryBuffer::pagesize * metaData->NextPageNo;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;

			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x + y < subjectID)
					continue;
				else if (x + y == subjectID)
					retBuffer->insertID(x);
				else
					return;
			}
		}
	}*/
}

void PartitionMaster::findSubjectIDByPredicateAndObject(const ID objectID,
		EntityIDBuffer *retBuffer, const ID minID, const ID maxID,
		const uchar* startPtr, const int xyType) {
/*#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif
	findObjectIDByPredicateAndSubject(objectID, retBuffer, minID, maxID,
			startPtr, xyType);
}

void PartitionMaster::findSubjectIDByPredicateAndObject(const ID objectID,
		EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
}

void PartitionMaster::findObjectIDAndSubjectIDByPredicate(
		EntityIDBuffer *retBuffer, const ID minID, const ID maxID,
		const uchar *startPtr, const int xyType) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	if (minID == 0 && maxID == UINT_MAX) {
		findObjectIDAndSubjectIDByPredicate(retBuffer, startPtr, xyType);
		return;
	}

	register ID x, y;
	const uchar *limit, *reader, *chunkBegin = startPtr;

	retBuffer->setIDCount(2);
	retBuffer->setSortKey(0);

	if (xyType == 1) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;

		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x < minID)
				continue;
			else if (x <= maxID) {
				retBuffer->insertID(x);
				retBuffer->insertID(x + y);
			} else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = reader + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x < minID)
					continue;
				else if (x <= maxID) {
					retBuffer->insertID(x);
					retBuffer->insertID(x + y);
				} else
					return;
			}
		}
	} else if (xyType == 2) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;

		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x + y < minID)
				continue;
			else if (x + y <= maxID) {
				retBuffer->insertID(x + y);
				retBuffer->insertID(x);
			} else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = reader + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x + y < minID)
					continue;
				else if (x + y <= maxID) {
					retBuffer->insertID(x + y);
					retBuffer->insertID(x);
				} else
					return;
			}
		}
	}*/
}

void PartitionMaster::findObjectIDAndSubjectIDByPredicate(
		EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
	/*register ID x, y;
	const uchar *reader, *limit, *chunkBegin = startPtr;

	retBuffer->setIDCount(2);
	retBuffer->setSortKey(0);

	if (xyType == 1) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = reader + metaData->usedSpace;
		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			retBuffer->insertID(x);
			retBuffer->insertID(x + y);
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = reader + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				retBuffer->insertID(x);
				retBuffer->insertID(x + y);
			}
		}
	} else if (xyType == 2) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = reader + metaData->usedSpace;
		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			retBuffer->insertID(x + y);
			retBuffer->insertID(x);
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = reader + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				retBuffer->insertID(x + y);
				retBuffer->insertID(x);
			}
		}
	}*/
}

void PartitionMaster::findSubjectIDAndObjectIDByPredicate(
		EntityIDBuffer *retBuffer, const ID minID, const ID maxID,
		const uchar *startPtr, const int xyType) {
/*#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif
	findObjectIDAndSubjectIDByPredicate(retBuffer, minID, maxID, startPtr,
			xyType);*/
}

void PartitionMaster::findSubjectIDAndObjectIDByPredicate(
		EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
}

void PartitionMaster::findObjectIDByPredicate(EntityIDBuffer *retBuffer,
		const ID minID, const ID maxID, const uchar* startPtr,
		const int xyType) {
/*#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif
	if (minID == 0 && maxID == UINT_MAX) {
		findObjectIDByPredicate(retBuffer, startPtr, xyType);
		return;
	}
	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	register ID x, y;
	const uchar *reader, *limit, *chunkBegin = startPtr;

	if (xyType == 1) {
		MetaData* metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;
		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x < minID)
				continue;
			else if (x <= maxID)
				retBuffer->insertID(x);
			else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			MetaData *metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x < minID)
					continue;
				else if (x <= maxID)
					retBuffer->insertID(x);
				else
					return;
			}
		}
	} else if (xyType == 2) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;
		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			if (x + y < minID)
				continue;
			else if (x + y <= maxID)
				retBuffer->insertID(x + y);
			else
				return;
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			MetaData *metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x + y < minID)
					continue;
				else if (x + y <= maxID)
					retBuffer->insertID(x + y);
				else
					return;
			}
		}
	}*/
}

void PartitionMaster::findObjectIDByPredicate(EntityIDBuffer *retBuffer,
		const uchar *startPtr, const int xyType) {
	/*retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	register ID x, y;
	const uchar *reader, *limit, *chunkBegin = startPtr;

	if (xyType == 1) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = reader + metaData->usedSpace;
		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			retBuffer->insertID(x);
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = reader + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				retBuffer->insertID(x);
			}
		}
	} else if (xyType == 2) {
		MetaData *metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData*);
		limit = reader + metaData->usedSpace;
		while (reader < limit) {
			reader = Chunk::readYId(Chunk::readXId(reader, x), y);
			retBuffer->insertID(x + y);
		}
		while (metaData->haveNextPage) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = reader + metaData->usedSpace;
			while (reader < limit) {
				reader = Chunk::readYId(Chunk::readXId(reader, x), y);
				retBuffer->insertID(x + y);
			}
		}
	}*/
}

void PartitionMaster::findSubjectIDByPredicate(EntityIDBuffer *retBuffer,
		const ID minID, const ID maxID, const uchar *startPtr,
		const int xyType) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif
	findObjectIDByPredicate(retBuffer, minID, maxID, startPtr, xyType);
}

void PartitionMaster::findSubjectIDByPredicate(EntityIDBuffer *retBuffer,
		const uchar *startPtr, const int xyType) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif
	findObjectIDByPredicate(retBuffer, startPtr, xyType);
}

double PartitionMaster::getChunkMinOrMax(const SOCouple* triple,
		const bool soType) {
	if (soType == ORDERBYS) {
		return triple->subjectID;
	} else {
		return triple->object;
	}
}
