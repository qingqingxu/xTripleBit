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
#include "comm/Tasks.h"
#include "TempBuffer.h"
#include "MMapBuffer.h"
#include "PartitionMaster.h"
#include "ThreadPool.h"
#include "TempMMapBuffer.h"
#include "util/Timestamp.h"

#define QUERY_TIME
#define MYDEBUG

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
/*
#ifdef MYDEBUG
		ofstream out;
		if (type == ORDERBYS) {
			out.open("chunkidmetadata_s", ios::app);
		} else {
			out.open("chunkidmetadata_o", ios::app);
		}
		MetaData* metaData0 = (MetaData*) xChunkQueue[type][0]->getChunkBegin();
		out << "partitionID," << partitionID << ",chunkID," << chunkID
				<< ",metaData.min," << metaData0->min << ",metaData.max,"
				<< metaData0->max << ",metaData.pageNo," << metaData0->pageNo
				<< endl;
#endif
*/
		xChunkTempBuffer[type][0] = new TempBuffer;
		for (chunkID = 1; chunkID < xChunkNumber[type]; chunkID++) {
			xChunkQueue[type][chunkID] = new TasksQueueChunk(
					startPtr + chunkID * MemoryBuffer::pagesize
							- sizeof(ChunkManagerMeta), chunkID, type);
/*
#ifdef MYDEBUG
			metaData0 = (MetaData*) xChunkQueue[type][chunkID]->getChunkBegin();
			out << "partitionID," << partitionID << ",chunkID," << chunkID
					<< ",metaData.min," << metaData0->min << ",metaData.max,"
					<< metaData0->max << ",metaData.pageNo,"
					<< metaData0->pageNo << endl;
#endif
*/
			xChunkTempBuffer[type][chunkID] = new TempBuffer;
		}
/*
#ifdef MYDEBUG
		out.close();
#endif
*/
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
	/*
	 #ifdef MYDEBUG
	 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
	 #endif
	 */

	while (1) {
		SubTrans* subTransaction = tasksQueue->Dequeue();

		if (subTransaction == NULL)
			break;

		switch (subTransaction->operationType) {
		case TripleBitQueryGraph::QUERY:
			//executeQuery(subTransaction);
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
			//executeDeleteClause(subTransaction);
			delete subTransaction;
			break;
		case TripleBitQueryGraph::UPDATE:
			SubTrans *subTransaction2 = tasksQueue->Dequeue();
			//executeUpdate(subTransaction, subTransaction2);
			delete subTransaction;
			delete subTransaction2;
			break;
		}
	}
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

/*
 void PartitionMaster::executeQuery(SubTrans *subTransaction){
 #ifdef QUERY_TIME
 Timestamp t1;
 #endif

 ID minID = subTransaction->minID;
 ID maxID = subTransaction->maxID;
 TripleNode *triple = &(subTransaction->triple);

 #ifdef MYDEBUG
 triple->print();
 cout << "minID: " << minID << "\tmaxID: " << minID <<endl;
 #endif

 size_t chunkCount, xChunkCount, xyChunkCount;
 size_t xChunkIDMin, xChunkIDMax, xyChunkIDMin, xyChunkIDMax;
 int soType, xyType;
 xChunkIDMin = xChunkIDMax = xyChunkIDMin = xyChunkIDMax = 0;
 chunkCount = xChunkCount = xyChunkCount = 0;

 switch (triple->scanOperation) {
 case TripleNode::FINDOSBYP: {
 soType = 1;
 xyType = 1;
 xChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, minID + 1);
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, UINT_MAX);
 assert(xChunkIDMin <= xChunkIDMax);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;

 xyType = 2;
 minID = subTransaction->minID;
 maxID = subTransaction->maxID;
 xyChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, 1);
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, maxID - 1);
 assert(xyChunkIDMin <= xyChunkIDMax);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;

 break;
 }
 case TripleNode::FINDSOBYP: {
 soType = 0;
 xyType = 1;
 xChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, minID + 1);
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, UINT_MAX);
 assert(xChunkIDMin <= xChunkIDMax);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;

 xyType = 2;
 minID = subTransaction->minID;
 maxID = subTransaction->maxID;
 xyChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, 1);
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, maxID - 1);
 assert(xyChunkIDMin <= xyChunkIDMax);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;

 break;
 }
 case TripleNode::FINDSBYPO: {
 soType = 1;
 if (minID > triple->object) {
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, minID, xChunkIDMin)) {
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, maxID, xChunkIDMax)) {
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 }
 } else
 xChunkCount = 0;
 } else if (maxID < triple->object) {
 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, minID, xyChunkIDMin)) {
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, maxID, xyChunkIDMax)) {
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;
 }
 } else
 xyChunkCount = 0;
 } else if (minID < triple->object && maxID > triple->object) {
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, triple->object + 1, xChunkIDMin)) {
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, maxID, xChunkIDMax)) {
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 }
 } else
 xChunkCount = 0;

 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, minID, xyChunkIDMin)) {
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->object, triple->object - 1, xyChunkIDMax)) {
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;
 }
 } else
 xyChunkCount = 0;
 }
 break;
 }
 case TripleNode::FINDOBYSP: {
 soType = 0;
 if (minID > triple->subject) {
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, minID, xChunkIDMin)) {
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, maxID);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 } else
 xChunkCount = 0;
 } else if (maxID < triple->subject) {
 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, minID, xyChunkIDMin)) {
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, maxID);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;
 } else
 xyChunkCount = 0;
 } else if (minID < triple->subject && maxID > triple->subject) {
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, triple->subject + 1, xChunkIDMin)) {
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, maxID);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 } else
 xChunkCount = 0;

 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, minID, xyChunkIDMin)) {
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(triple->subject, triple->subject - 1);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;
 } else
 xyChunkCount = 0;
 }
 break;
 }
 case TripleNode::FINDSBYP: {
 soType = 0;
 xyType = 1;
 xChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, minID + 1);
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, UINT_MAX);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;

 xyType = 2;
 minID = subTransaction->minID;
 maxID = subTransaction->maxID;
 xyChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, 1);
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, maxID - 1);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;

 break;
 }
 case TripleNode::FINDOBYP: {
 soType = 1;
 xyType = 1;
 xChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, minID + 1);
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, UINT_MAX);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;

 xyType = 2;
 minID = subTransaction->minID;
 maxID = subTransaction->maxID;
 xyChunkIDMin = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(minID, 1);
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(maxID, maxID - 1);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;

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
 new subTaskPackage(chunkCount, subTransaction->operationType, sourceWorkerID, subTransaction->minID, subTransaction->maxID, 0, 0,
 partitionBufferManager));

 #ifdef QUERY_TIME
 Timestamp t2;
 cout << "find chunks time elapsed: " << (static_cast<double> (t2 - t1) / 1000.0) << " s" << endl;
 Timestamp t3;
 #endif

 ChunkTask *chunkTask = new ChunkTask(subTransaction->operationType, triple->subject, triple->object, triple->scanOperation, taskPackage,
 subTransaction->indexForTT);
 if (xChunkCount != 0) {
 for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax; offsetID++) {
 taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
 }
 }
 if (xyChunkCount != 0) {
 for (size_t offsetID = xyChunkIDMin; offsetID <= xyChunkIDMax; offsetID++) {
 taskEnQueue(chunkTask, xyChunkQueue[soType][offsetID]);
 }
 }
 #ifdef QUERY_TIME
 Timestamp t4;
 cout << "ChunkCount:" << chunkCount << " taskEnqueue time elapsed: " << (static_cast<double> (t4-t3)/1000.0) << " s" << endl;
 #endif
 }
 */

void PartitionMaster::executeInsertData(SubTrans* subTransaction) {
	/*
	 #ifdef MYDEBUG
	 cout << __FUNCTION__ << endl;
	 #endif
	 */
	ID subjectID = subTransaction->triple.subjectID;
	double object = subTransaction->triple.object;
	char objType = subTransaction->triple.objType;
	size_t chunkID;
	shared_ptr<subTaskPackage> taskPackage(new subTaskPackage);

	/*ofstream s("searchChunkIDByS", ios::app);
	s << "partitionID, " << partitionID << ",subjectID, " << subjectID
			<< ",object, " << object;*/
	chunkID = partitionChunkManager[ORDERBYS]->getChunkIndex()->searchChunk(
			subjectID, object);
	/*s << ",chunkID, " << chunkID << endl;
	s.close();*/
	ChunkTask *chunkTask1 = new ChunkTask(subTransaction->operationType,
			subjectID, object, objType, subTransaction->triple.scanOperation,
			taskPackage, subTransaction->indexForTT);
	taskEnQueue(chunkTask1, xChunkQueue[ORDERBYS][chunkID]);

	/*ofstream o("searchChunkIDByO", ios::app);
	o << "partitionID, " << partitionID << ",object, " << object
			<< ",subjectID, " << subjectID;*/
	chunkID = partitionChunkManager[ORDERBYO]->getChunkIndex()->searchChunk(
			object, subjectID);
	/*o << ",chunkID, " << chunkID << endl;
	o.close();*/

	ChunkTask *chunkTask2 = new ChunkTask(subTransaction->operationType,
			subjectID, object, objType, subTransaction->triple.scanOperation,
			taskPackage, subTransaction->indexForTT);
	taskEnQueue(chunkTask2, xChunkQueue[ORDERBYO][chunkID]);
}

void PartitionMaster::executeDeleteData(SubTrans* subTransaction) {
	/*
	 #ifdef MYDEBUG
	 cout << __FUNCTION__ << endl;
	 #endif
	 */
	executeInsertData(subTransaction);
}

/*
 void PartitionMaster::executeDeleteClause(SubTrans* subTransaction) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << endl;
 #endif
 ID subjectID = subTransaction->triple.subject;
 ID objectID = subTransaction->triple.object;
 int soType, xyType;
 size_t xChunkIDMin, xChunkIDMax, xyChunkIDMin, xyChunkIDMax;
 size_t chunkCount, xChunkCount, xyChunkCount;
 xChunkIDMin = xChunkIDMax = xyChunkIDMin = xyChunkIDMax = 0;
 chunkCount = xChunkCount = xyChunkCount = 0;

 if (subTransaction->triple.constSubject) {
 soType = 0;
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, subjectID + 1, xChunkIDMin)) {
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, UINT_MAX);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 } else
 xChunkCount = 0;

 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, 0, xyChunkIDMin)) {
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, subjectID - 1);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;
 } else {
 xyChunkCount = 0;
 }

 if (xChunkCount + xyChunkCount == 0)
 return;

 chunkCount = xChunkCount + xyChunkCount;
 shared_ptr<subTaskPackage> taskPackage(
 new subTaskPackage(chunkCount, subTransaction->operationType, 0, 0, 0, subjectID, 0, partitionBufferManager));
 if (xChunkCount != 0) {
 for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransaction->operationType, subjectID, objectID, subTransaction->triple.scanOperation,
 taskPackage, subTransaction->indexForTT);
 taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
 }
 }
 if (xyChunkCount != 0) {
 for (size_t offsetID = xyChunkIDMin; offsetID <= xyChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransaction->operationType, subjectID, objectID, subTransaction->triple.scanOperation,
 taskPackage, subTransaction->indexForTT);
 taskEnQueue(chunkTask, xyChunkQueue[soType][offsetID]);
 }
 }
 } else {
 soType = 1;
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, objectID + 1, xChunkIDMin)) {
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, UINT_MAX);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 } else
 xChunkCount = 0;

 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, 0, xyChunkIDMin)) {
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, objectID - 1);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkIDMax - xyChunkIDMin + 1;
 } else
 xyChunkCount = 0;

 if (xChunkCount + xyChunkCount == 0)
 return;
 chunkCount = xChunkCount + xyChunkCount;
 shared_ptr<subTaskPackage> taskPackage(
 new subTaskPackage(chunkCount, subTransaction->operationType, 0, 0, 0, objectID, 0, partitionBufferManager));
 if (xChunkCount != 0) {
 for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransaction->operationType, subjectID, objectID, subTransaction->triple.scanOperation,
 taskPackage, subTransaction->indexForTT);
 taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
 }
 }
 if (xyChunkCount != 0) {
 for (size_t offsetID = xyChunkIDMin; offsetID <= xyChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransaction->operationType, subjectID, objectID, subTransaction->triple.scanOperation,
 taskPackage, subTransaction->indexForTT);
 taskEnQueue(chunkTask, xyChunkQueue[soType][offsetID]);
 }
 }
 }
 }

 void PartitionMaster::executeUpdate(SubTrans *subTransfirst, SubTrans *subTranssecond) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << endl;
 #endif
 ID subjectID = subTransfirst->triple.subject;
 ID objectID = subTransfirst->triple.object;
 ID subUpdate = subTranssecond->triple.subject;
 ID obUpdate = subTranssecond->triple.object;
 int soType, xyType;
 size_t xChunkIDMin, xChunkIDMax, xyChunkIDMin, xyChunkIDMax;
 size_t chunkCount, xChunkCount, xyChunkCount;
 xChunkIDMin = xChunkIDMax = xyChunkIDMin = xyChunkIDMax = 0;
 chunkCount = xChunkCount = xyChunkCount = 0;

 if (subTransfirst->triple.constSubject) {
 soType = 0;
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, subjectID + 1, xChunkIDMin)) {
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, UINT_MAX);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 } else
 xChunkCount = 0;

 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, 0, xyChunkIDMin)) {
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(subjectID, subjectID - 1);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkCount - xyChunkCount + 1;
 } else{
 xyChunkCount = 0;
 }

 if (xChunkCount + xyChunkCount == 0)
 return;
 chunkCount = xChunkCount + xyChunkCount;
 shared_ptr<subTaskPackage> taskPackage(
 new subTaskPackage(chunkCount, subTransfirst->operationType, 0, 0, 0, subjectID, subUpdate, partitionBufferManager));
 if (xChunkCount != 0) {
 for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransfirst->operationType, subjectID, objectID, subTransfirst->triple.scanOperation,
 taskPackage, subTransfirst->indexForTT);
 taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
 }
 }
 if (xyChunkCount != 0) {
 for (size_t offsetID = xyChunkIDMin; offsetID <= xyChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransfirst->operationType, subjectID, objectID, subTransfirst->triple.scanOperation,
 taskPackage, subTransfirst->indexForTT);
 taskEnQueue(chunkTask, xyChunkQueue[soType][offsetID]);
 }
 }
 } else {
 soType = 1;
 xyType = 1;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, objectID + 1, xChunkIDMin)) {
 xChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, UINT_MAX);
 assert(xChunkIDMax >= xChunkIDMin);
 xChunkCount = xChunkIDMax - xChunkIDMin + 1;
 } else
 xChunkCount = 0;

 xyType = 2;
 if (partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, 0, xyChunkIDMin)) {
 xyChunkIDMax = partitionChunkManager[soType]->getChunkIndex(xyType)->searchChunk(objectID, objectID - 1);
 assert(xyChunkIDMax >= xyChunkIDMin);
 xyChunkCount = xyChunkCount - xyChunkCount + 1;
 } else
 xyChunkCount = 0;

 if (xChunkCount + xyChunkCount == 0)
 return;
 chunkCount = xChunkCount + xyChunkCount;
 shared_ptr<subTaskPackage> taskPackage(
 new subTaskPackage(chunkCount, subTransfirst->operationType, 0, 0, 0, objectID, obUpdate, partitionBufferManager));
 if (xChunkCount != 0) {
 for (size_t offsetID = xChunkIDMin; offsetID <= xChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransfirst->operationType, subjectID, objectID, subTransfirst->triple.scanOperation,
 taskPackage, subTransfirst->indexForTT);
 taskEnQueue(chunkTask, xChunkQueue[soType][offsetID]);
 }
 }
 if (xyChunkCount != 0) {
 for (size_t offsetID = xyChunkIDMin; offsetID <= xyChunkIDMax; offsetID++) {
 ChunkTask *chunkTask = new ChunkTask(subTransfirst->operationType, subjectID, objectID, subTransfirst->triple.scanOperation,
 taskPackage, subTransfirst->indexForTT);
 taskEnQueue(chunkTask, xyChunkQueue[soType][offsetID]);
 }
 }
 }
 }
 */

void PrintChunkTaskPart(ChunkTask* chunkTask) {
	cout << "opType:" << chunkTask->operationType << " subject:"
			<< chunkTask->Triple.subjectID << " object:"
			<< chunkTask->Triple.object << " operation:"
			<< chunkTask->Triple.operation << endl;
}

void PartitionMaster::handleTasksQueueChunk(TasksQueueChunk* tasksQueue) {
	/*
	 #ifdef MYDEBUG
	 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
	 #endif
	 */

	ChunkTask* chunkTask = NULL;
	ID chunkID = tasksQueue->getChunkID();
	int xyType = 0; //tasksQueue->getXYType();
	int soType = tasksQueue->getSOType();
	const uchar* chunkBegin = tasksQueue->getChunkBegin();

	while ((chunkTask = tasksQueue->Dequeue()) != NULL) {
		switch (chunkTask->operationType) {
		case TripleBitQueryGraph::QUERY:
			//executeChunkTaskQuery(chunkTask, chunkID, chunkBegin, xyType);
			break;
		case TripleBitQueryGraph::INSERT_DATA:
			executeChunkTaskInsertData(chunkTask, chunkID, chunkBegin, soType);
			break;
		case TripleBitQueryGraph::DELETE_DATA:
			executeChunkTaskDeleteData(chunkTask, chunkID, chunkBegin, soType);
			break;
		case TripleBitQueryGraph::DELETE_CLAUSE:
			//executeChunkTaskDeleteClause(chunkTask, chunkID, chunkBegin, xyType, soType);
			break;
		case TripleBitQueryGraph::UPDATE:
			//executeChunkTaskUpdate(chunkTask, chunkID, chunkBegin, xyType, soType);
			break;
		}
	}
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
		metaData->min = min;
		metaData->max = max;
	}

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
/*
#ifdef MYDEBUG
	ofstream out("tempbuffer", ios::app);
	ChunkTriple *temp = buffer->getBuffer();
	while(temp < buffer->getEnd()){
		out << temp->subjectID << "," << partitionID << "," << temp->object << endl;
		temp++;
	}
	out.close();
#endif
*/
	buffer->sort(soType);
/*
#ifdef MYDEBUG
	ofstream out2("tempbuffer_sort", ios::app);
	temp = buffer->getBuffer();
	while(temp < buffer->getEnd()){
		out2 << temp->subjectID << "," << partitionID << "," << temp->object << endl;
		temp++;
	}
	out2.close();
#endif
*/
	buffer->uniqe();
#ifdef MYDEBUG
	ofstream out1("tempbuffer_uniqe7", ios::app);
	ChunkTriple* temp = buffer->getBuffer();
	while(temp < buffer->getEnd()){
		out1 << temp->subjectID << "," << partitionID << "," << temp->object << endl;
		temp++;
	}
	out1.close();
#endif

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
	memset(tempPage, 0, MemoryBuffer::pagesize);
	memset(tempPage2, 0, MemoryBuffer::pagesize);

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

	ChunkTriple* chunkTriple, *tempTriple;
	ChunkTriple *lastTempBuffer, *currentTempBuffer, *endTempBuffer;
	ChunkTriple *start = buffer->getBuffer(), *end = buffer->getEnd();
	lastTempBuffer = currentTempBuffer = start;
	endTempBuffer = end;

	chunkTriple = (ChunkTriple*) malloc(sizeof(ChunkTriple));
	if (chunkTriple == NULL) {
		cout << "malloc a ChunkTriple error" << endl;
		free(chunkTriple);
		return;
	}
	memset(chunkTriple, 0, sizeof(ChunkTriple));
	tempTriple = currentTempBuffer;
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
				//cout << "chunkTriple: " << chunkTriple->subjectID << "\t" << chunkTriple->object << endl;
				//cout << "tempTriple: " << tempTriple->subjectID << "\t" << tempTriple->object << endl;
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
				currentPtrChunk += len;
				//					assert(currentPtrChunk <= endPtrChunk);

				lastTempBuffer = currentTempBuffer;
				currentTempBuffer++;
				if (currentTempBuffer < endTempBuffer) {
					tempTriple = currentTempBuffer;
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
		//cout << tempTriple->subjectID << "\t" << tempTriple->object << endl;
		partitionChunkManager[soType]->writeXY(currentPtrChunk,
				tempTriple->subjectID, tempTriple->object, tempTriple->objType);
		max = getChunkMinOrMax(tempTriple, soType);
		currentPtrChunk += len;
		//			assert(currentPtrChunk <= endPtrChunk);

		lastTempBuffer = currentTempBuffer;
		currentTempBuffer++;
		if (currentTempBuffer < endTempBuffer) {
			tempTriple = currentTempBuffer;
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
	/*
	 #ifdef MYDEBUG
	 cout << __FUNCTION__ << endl;
	 MetaData* metaData0 = (MetaData*)xChunkQueue[soType][chunkID]->getChunkBegin();
	 cout << "metaData.min," << metaData0->min << ",metaData.max," << metaData0->max<< ",metaData.pageNo," << metaData0->pageNo << endl;
	 #endif
	 */
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

/*
 void PartitionMaster::deleteDataForDeleteClause(EntityIDBuffer *buffer, const ID deleteID, const bool soType) {
 size_t size = buffer->getSize();
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
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(1)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(operationType, deleteID, retBuffer[i], scanType, taskPackage, indexForTT);
 xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }
 for (size_t i = index; i < size; ++i) {
 //object >subject == x>y
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(2)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(operationType, deleteID, retBuffer[i], scanType, taskPackage, indexForTT);
 xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }
 } else if (soType == 1) {
 //deleteID -->object
 int deleteSOType = 0;
 for (size_t i = 0; i < index; ++i) {
 //subject <object== x<y
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(1)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(operationType, retBuffer[i], deleteID, scanType, taskPackage, indexForTT);
 xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }
 for (size_t i = index; i < size; ++i) {
 //subject > object == x >y
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(2)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(operationType, retBuffer[i], deleteID, scanType, taskPackage, indexForTT);
 xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }
 }
 }

 void PartitionMaster::executeChunkTaskDeleteClause(ChunkTask *chunkTask, const ID chunkID, const uchar *startPtr, const int xyType,
 const int soType) {
 ID deleteXID = 0, deleteXYID = 0;
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
 if (xyType == 1) {
 //x<y
 MetaData *metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 temp = const_cast<uchar*>(reader);
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (x < deleteXID)
 continue;
 else if (x == deleteXID) {
 retBuffer->insertID(y);
 temp = Chunk::deleteYId(Chunk::deleteXId(temp));
 } else
 goto END;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 temp = const_cast<uchar*>(reader);
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (x < deleteXID)
 continue;
 else if (x == deleteXID) {
 retBuffer->insertID(y);
 temp = Chunk::deleteYId(Chunk::deleteXId(temp));
 } else
 goto END;
 }
 }
 } else if (xyType == 2) {
 //x>y
 MetaData *metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 temp = const_cast<uchar*>(reader);
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < deleteXYID)
 continue;
 else if (y == deleteXYID) {
 retBuffer->insertID(x);
 temp = Chunk::deleteYId(Chunk::deleteXId(temp));
 } else
 goto END;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 temp = const_cast<uchar*>(reader);
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < deleteXYID)
 continue;
 else if (y == deleteXYID) {
 retBuffer->insertID(x);
 temp = Chunk::deleteYId(Chunk::deleteXId(temp));
 } else
 goto END;
 }
 }
 }

 END:
 //	chunkTask->taskPackage->completeSubTask(chunkID, retBuffer, xyType);
 if (chunkTask->taskPackage->completeSubTask(chunkID, retBuffer, xyType)) {
 EntityIDBuffer *buffer = chunkTask->taskPackage->getTaskResult();
 ID deleteID = chunkTask->taskPackage->deleteID;
 deleteDataForDeleteClause(buffer, deleteID, soType);

 partitionBufferManager->freeBuffer(buffer);
 }
 retBuffer = NULL;
 }

 void PartitionMaster::updateDataForUpdate(EntityIDBuffer *buffer, const ID deleteID, const ID updateID, const int soType) {
 size_t size = buffer->getSize();
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

 int deleteSOType, insertSOType, xyType;
 if (soType == 0) {
 //deleteID -->subject
 deleteSOType = 1;
 xyType = 1;
 for (size_t i = 0; i < indexDelete; ++i) {
 //object < subject == x<y
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID, retBuffer[i], scanType, taskPackage, indexForTT);
 xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }
 xyType = 2;
 for (size_t i = indexDelete; i < size; ++i) {
 //object > subject == x>y
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID, retBuffer[i], scanType, taskPackage, indexForTT);
 xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }

 for (size_t i = 0; i < indexUpdate; ++i) {
 //subject > object
 insertSOType = 0;
 xyType = 2;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(updateID, retBuffer[i]);
 ChunkTask *chunkTask1 = new ChunkTask(opInsert, updateID, retBuffer[i], scanType, taskPackage, indexForTT);
 xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
 insertSOType = 1;
 xyType = 1;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], updateID);
 ChunkTask *chunkTask2 = new ChunkTask(opInsert, updateID, retBuffer[i], scanType, taskPackage, indexForTT);
 xChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
 }
 for (size_t i = indexUpdate; i < size; ++i) {
 //subject < object
 insertSOType = 0;
 xyType = 1;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(updateID, retBuffer[i]);
 ChunkTask *chunkTask1 = new ChunkTask(opInsert, updateID, retBuffer[i], scanType, taskPackage, indexForTT);
 xChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
 insertSOType = 1;
 xyType = 2;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], updateID);
 ChunkTask *chunkTask2 = new ChunkTask(opInsert, updateID, retBuffer[i], scanType, taskPackage, indexForTT);
 xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
 }
 } else if (soType == 1) {
 //deleteID-->object
 deleteSOType = 0;
 xyType = 1;
 for (size_t i = 0; i < indexDelete; ++i) {
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID, retBuffer[i], scanType, taskPackage, indexForTT);
 xChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }
 xyType = 2;
 for (size_t i = indexDelete; i < size; ++i) {
 chunkID = partitionChunkManager[deleteSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], deleteID);
 ChunkTask *chunkTask = new ChunkTask(opDelete, deleteID, retBuffer[i], scanType, taskPackage, indexForTT);
 xyChunkQueue[deleteSOType][chunkID]->EnQueue(chunkTask);
 }

 for (size_t i = 0; i < indexUpdate; ++i) {
 //subject < object
 insertSOType = 0, xyType = 1;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], updateID);
 ChunkTask *chunkTask1 = new ChunkTask(opInsert, retBuffer[i], updateID, scanType, taskPackage, indexForTT);
 xChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
 insertSOType = 1, xyType = 2;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(updateID, retBuffer[i]);
 ChunkTask *chunkTask2 = new ChunkTask(opInsert, retBuffer[i], updateID, scanType, taskPackage, indexForTT);
 xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
 }
 for (size_t i = indexUpdate; i < size; ++i) {
 //subject > object
 insertSOType = 0;
 xyType = 2;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(retBuffer[i], updateID);
 ChunkTask *chunkTask1 = new ChunkTask(opInsert, retBuffer[i], updateID, scanType, taskPackage, indexForTT);
 xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask1);
 insertSOType = 1;
 xyType = 1;
 chunkID = partitionChunkManager[insertSOType]->getChunkIndex(xyType)->searchChunk(updateID, retBuffer[i]);
 ChunkTask *chunkTask2 = new ChunkTask(opInsert, retBuffer[i], updateID, scanType, taskPackage, indexForTT);
 xyChunkQueue[insertSOType][chunkID]->EnQueue(chunkTask2);
 }
 }
 }

 void PartitionMaster::executeChunkTaskUpdate(ChunkTask *chunkTask, const ID chunkID, const uchar* startPtr, const int xyType, const int soType) {
 ID deleteXID = 0, deleteXYID = 0;
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
 retBuffer->insertID(y);
 temp = Chunk::deleteYId(Chunk::deleteXId(temp));
 } else
 goto END;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 temp = const_cast<uchar*>(reader);
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (x < deleteXID)
 continue;
 else if (x == deleteXID) {
 retBuffer->insertID(y);
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
 if (y < deleteXYID)
 continue;
 else if (y == deleteXYID) {
 retBuffer->insertID(x);
 temp = Chunk::deleteYId(Chunk::deleteXId(temp));
 } else
 goto END;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 temp = const_cast<uchar*>(reader);
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < deleteXYID)
 continue;
 else if (y == deleteXYID) {
 retBuffer->insertID(x);
 temp = Chunk::deleteYId(Chunk::deleteXId(temp));
 } else
 goto END;
 }
 }
 }
 END: if (chunkTask->taskPackage->completeSubTask(chunkID, retBuffer, xyType)) {
 #ifdef MYTESTDEBUG
 cout << "complete all task update" << endl;
 #endif
 EntityIDBuffer *buffer = chunkTask->taskPackage->getTaskResult();
 ID deleteID = chunkTask->taskPackage->deleteID;
 ID updateID = chunkTask->taskPackage->updateID;
 updateDataForUpdate(buffer, deleteID, updateID, soType);

 partitionBufferManager->freeBuffer(buffer);
 }
 retBuffer = NULL;
 }

 void PartitionMaster::executeChunkTaskQuery(ChunkTask *chunkTask, const ID chunkID, const uchar* chunkBegin, const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
 #endif

 //	EntityIDBuffer* retBuffer = partitionBufferManager->getNewBuffer();
 EntityIDBuffer *retBuffer = new EntityIDBuffer;
 retBuffer->empty();

 switch (chunkTask->Triple.operation) {
 case TripleNode::FINDSBYPO:
 findSubjectIDByPredicateAndObject(chunkTask->Triple.object, retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID,
 chunkBegin, xyType);
 break;
 case TripleNode::FINDOBYSP:
 findObjectIDByPredicateAndSubject(chunkTask->Triple.subjectID, retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID,
 chunkBegin, xyType);
 break;
 case TripleNode::FINDSBYP:
 findSubjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
 break;
 case TripleNode::FINDOBYP:
 findObjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
 break;
 case TripleNode::FINDOSBYP:
 findObjectIDAndSubjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
 break;
 case TripleNode::FINDSOBYP:
 findSubjectIDAndObjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
 break;
 default:
 cout << "unsupport now! executeChunkTaskQuery" << endl;
 break;
 }

 if (chunkTask->taskPackage->completeSubTask(chunkID, retBuffer, xyType)) {
 //		EntityIDBuffer* buffer = chunkTask->taskPackage->getTaskResult();
 ResultIDBuffer* buffer = new ResultIDBuffer(chunkTask->taskPackage);

 resultBuffer[chunkTask->taskPackage->sourceWorkerID]->EnQueue(buffer);
 }
 retBuffer = NULL;
 }

 void PartitionMaster::findObjectIDByPredicateAndSubject(const ID subjectID, EntityIDBuffer *retBuffer, const ID minID, const ID maxID,
 const uchar* startPtr, const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
 cout << subjectID << "\t" << minID << "\t" << maxID << "\t" << xyType << endl;
 #endif

 if (minID == 0 && maxID == UINT_MAX) {
 findObjectIDByPredicateAndSubject(subjectID, retBuffer, startPtr, xyType);
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
 if (y < minID)
 continue;
 else if (y <= maxID){
 retBuffer->insertID(y);
 }
 else{
 return;
 }

 } else{
 return;
 }
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + MemoryBuffer::pagesize * metaData->NextPageNo;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;

 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (x < subjectID)
 continue;
 else if (x == subjectID) {
 if (y < minID)
 continue;
 else if (y <= maxID){
 retBuffer->insertID(y);
 }
 else{
 return;
 }
 } else{
 return;
 }
 }
 }
 } else if (xyType == 2) {
 MetaData* metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < subjectID)
 continue;
 else if (y == subjectID) {
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
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + MemoryBuffer::pagesize * metaData->NextPageNo;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < subjectID)
 continue;
 else if (y == subjectID) {
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
 }
 }

 void PartitionMaster::findObjectIDByPredicateAndSubject(const ID subjectID, EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
 register ID x, y;
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
 retBuffer->insertID(y);
 else
 return;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + MemoryBuffer::pagesize * metaData->NextPageNo;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;

 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (x < subjectID)
 continue;
 else if (x == subjectID)
 retBuffer->insertID(y);
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
 if (y < subjectID)
 continue;
 else if (y == subjectID)
 retBuffer->insertID(x);
 else
 return;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + MemoryBuffer::pagesize * metaData->NextPageNo;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;

 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < subjectID)
 continue;
 else if (y == subjectID)
 retBuffer->insertID(x);
 else
 return;
 }
 }
 }
 }

 void PartitionMaster::findSubjectIDByPredicateAndObject(const ID objectID, EntityIDBuffer *retBuffer, const ID minID, const ID maxID,
 const uchar* startPtr, const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
 #endif
 findObjectIDByPredicateAndSubject(objectID, retBuffer, minID, maxID, startPtr, xyType);
 }

 void PartitionMaster::findSubjectIDByPredicateAndObject(const ID objectID, EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
 }

 void PartitionMaster::findObjectIDAndSubjectIDByPredicate(EntityIDBuffer *retBuffer, const ID minID, const ID maxID, const uchar *startPtr,
 const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
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
 retBuffer->insertID(y);
 } else
 return;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = reader + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (x < minID)
 continue;
 else if (x <= maxID) {
 retBuffer->insertID(x);
 retBuffer->insertID(y);
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
 if (y < minID)
 continue;
 else if (y <= maxID) {
 retBuffer->insertID(y);
 retBuffer->insertID(x);
 } else
 return;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = reader + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < minID)
 continue;
 else if (y <= maxID) {
 retBuffer->insertID(y);
 retBuffer->insertID(x);
 } else
 return;
 }
 }
 }
 }

 void PartitionMaster::findObjectIDAndSubjectIDByPredicate(EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
 register ID x, y;
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
 retBuffer->insertID(y);
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = reader + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 retBuffer->insertID(x);
 retBuffer->insertID(y);
 }
 }
 } else if (xyType == 2) {
 MetaData *metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = reader + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 retBuffer->insertID(y);
 retBuffer->insertID(x);
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = reader + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 retBuffer->insertID(y);
 retBuffer->insertID(x);
 }
 }
 }
 }

 void PartitionMaster::findSubjectIDAndObjectIDByPredicate(EntityIDBuffer *retBuffer, const ID minID, const ID maxID, const uchar *startPtr,
 const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
 #endif
 findObjectIDAndSubjectIDByPredicate(retBuffer, minID, maxID, startPtr, xyType);
 }

 void PartitionMaster::findSubjectIDAndObjectIDByPredicate(EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
 }

 void PartitionMaster::findObjectIDByPredicate(EntityIDBuffer *retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
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
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
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
 if (y < minID)
 continue;
 else if (y <= maxID)
 retBuffer->insertID(y);
 else
 return;
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 MetaData *metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = chunkBegin + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 if (y < minID)
 continue;
 else if (y <= maxID)
 retBuffer->insertID(y);
 else
 return;
 }
 }
 }
 }

 void PartitionMaster::findObjectIDByPredicate(EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
 retBuffer->setIDCount(1);
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
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
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
 retBuffer->insertID(y);
 }
 while (metaData->haveNextPage) {
 chunkBegin = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + metaData->NextPageNo * MemoryBuffer::pagesize;
 metaData = (MetaData*) chunkBegin;
 reader = chunkBegin + sizeof(MetaData);
 limit = reader + metaData->usedSpace;
 while (reader < limit) {
 reader = Chunk::readYId(Chunk::readXId(reader, x), y);
 retBuffer->insertID(y);
 }
 }
 }
 }

 void PartitionMaster::findSubjectIDByPredicate(EntityIDBuffer *retBuffer, const ID minID, const ID maxID, const uchar *startPtr, const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
 #endif
 findObjectIDByPredicate(retBuffer, minID, maxID, startPtr, xyType);
 }

 void PartitionMaster::findSubjectIDByPredicate(EntityIDBuffer *retBuffer, const uchar *startPtr, const int xyType) {
 #ifdef MYDEBUG
 cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
 #endif
 findObjectIDByPredicate(retBuffer, startPtr, xyType);
 }
 */

double PartitionMaster::getChunkMinOrMax(const ChunkTriple* triple,
		const bool soType) {
	if (soType == ORDERBYS) {
		return triple->subjectID;
	} else {
		return triple->object;
	}
}
