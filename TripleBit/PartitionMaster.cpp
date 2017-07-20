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
//#define MYDEBUG

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
		if (!xChunkTempBuffer[soType][chunkID]->isEmpty()) {
			combineTempBufferToSource(xChunkTempBuffer[soType][chunkID],
					startPtr, chunkID, soType);
		}

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
	 cout << __FUNCTION__ << " partitionID: " << partitionID << "-----------> "
	 << pthread_self() << endl;
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
			subTransaction->indexForTT->completeOneTriple();
			delete subTransaction;
			break;
		case TripleBitQueryGraph::DELETE_CLAUSE:
			executeDeleteClause(subTransaction);
			subTransaction->indexForTT->completeOneTriple();
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
		cout << __FUNCTION__ << endl;
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
	shared_ptr<IndexForTT> indexForTT(new IndexForTT(2));
	ChunkTask *chunkTask1 = new ChunkTask(subTransaction->operationType,
			subjectID, object, objType, subTransaction->triple.scanOperation,
			taskPackage, indexForTT);
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
			taskPackage, indexForTT);
	taskEnQueue(chunkTask2, xChunkQueue[ORDERBYO][chunkID]);
	indexForTT->wait();
}

void PartitionMaster::executeDeleteData(SubTrans* subTransaction) {
	executeInsertData(subTransaction);
}

void PartitionMaster::executeDeleteClause(SubTrans* subTransaction) {
	ID subjectID =
			subTransaction->triple.constSubject ?
					subTransaction->triple.subjectID : 0;
	double object =
			subTransaction->triple.constObject ?
					subTransaction->triple.object : 0;
	char objType =
			subTransaction->triple.constObject ?
					subTransaction->triple.objType : NONE;

	size_t chunkCount = 0, chunkIDMin = 0, chunkIDMax = 0;

	if (!subTransaction->triple.constSubject
			&& !subTransaction->triple.constObject) {
		chunkIDMax =
				partitionChunkManager[ORDERBYS]->getChunkIndex()->getTableSize()
						- 1;
		chunkCount = chunkIDMax - chunkIDMin + 1;

		size_t chunkCountO = 0, chunkIDMinO = 0, chunkIDMaxO = 0;
		chunkIDMaxO =
				partitionChunkManager[ORDERBYO]->getChunkIndex()->getTableSize()
						- 1;
		chunkCountO = chunkIDMaxO - chunkIDMinO + 1;

		shared_ptr<IndexForTT> indexForTT(
				new IndexForTT(chunkCount + chunkCountO));

		shared_ptr<SubTaskPackageForDelete> taskPackage(
				new SubTaskPackageForDelete(chunkCount,
						subTransaction->operationType, subjectID,
						subTransaction->triple.constSubject,
						subTransaction->triple.constObject));
		if (chunkCount != 0) {
			cout << "Chunk count: " << chunkCount << endl;

			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransaction->operationType, subjectID, object,
						objType, subTransaction->triple.scanOperation,
						taskPackage, indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[ORDERBYS][offsetID]);
			}

		}

		shared_ptr<SubTaskPackageForDelete> taskPackageO(
				new SubTaskPackageForDelete(chunkCountO,
						subTransaction->operationType, object, objType,
						subTransaction->triple.constSubject,
						subTransaction->triple.constObject));
		if (chunkCountO != 0) {
			for (size_t offsetID = chunkIDMinO; offsetID <= chunkIDMaxO;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransaction->operationType, subjectID, object,
						objType, subTransaction->triple.scanOperation,
						taskPackageO, indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[ORDERBYO][offsetID]);
			}
		}

		indexForTT->wait();

	} else if (subTransaction->triple.constSubject) {
		//subject已知、object无论是否已知，均先处理subject的删除
		if (partitionChunkManager[ORDERBYS]->getChunkIndex()->searchChunk(
				subjectID, subjectID + 1, chunkIDMin)) {
			chunkIDMax =
					partitionChunkManager[ORDERBYS]->getChunkIndex()->searchChunk(
							subjectID, UINT_MAX);
			assert(chunkIDMax >= chunkIDMin);

		} else {
			return;
		}

		chunkCount = chunkIDMax - chunkIDMin + 1;

		if (chunkCount != 0) {
			cout << "constSubject Chunk count: " << chunkCount << endl;
			shared_ptr<SubTaskPackageForDelete> taskPackage(
					new SubTaskPackageForDelete(chunkCount,
							subTransaction->operationType, subjectID,
							subTransaction->triple.constSubject,
							subTransaction->triple.constObject));
			shared_ptr<IndexForTT> indexForTT(new IndexForTT(chunkCount));
			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransaction->operationType, subjectID, object,
						objType, subTransaction->triple.scanOperation,
						taskPackage, indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[ORDERBYS][offsetID]);
			}
			indexForTT->wait();
		}
	} else if (!subTransaction->triple.constSubject
			&& subTransaction->triple.constObject) {
		if (partitionChunkManager[ORDERBYO]->getChunkIndex()->searchChunk(
				object, object + 1, chunkIDMin)) {
			chunkIDMax =
					partitionChunkManager[ORDERBYO]->getChunkIndex()->searchChunk(
							object, UINT_MAX);
			assert(chunkIDMax >= chunkIDMin);
			chunkCount = chunkIDMax - chunkIDMin + 1;
		} else {
			return;
		}

		if (chunkCount != 0) {
			cout << "constObject Chunk count: " << chunkCount << "\tobject: "
					<< object << endl;
			shared_ptr<SubTaskPackageForDelete> taskPackage(
					new SubTaskPackageForDelete(chunkCount,
							subTransaction->operationType, object, objType,
							subTransaction->triple.constSubject,
							subTransaction->triple.constObject));
			shared_ptr<IndexForTT> indexForTT(new IndexForTT(chunkCount));
			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax;
					offsetID++) {
				ChunkTask *chunkTask = new ChunkTask(
						subTransaction->operationType, subjectID, object,
						objType, subTransaction->triple.scanOperation,
						taskPackage, indexForTT);
				taskEnQueue(chunkTask, xChunkQueue[ORDERBYO][offsetID]);
			}
			indexForTT->wait();
		}
	}
}

/*void PartitionMaster::executeUpdate(SubTrans *subTransfirst, SubTrans *subTranssecond) {
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
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	ChunkTask* chunkTask = NULL;
	ID chunkID = tasksQueue->getChunkID();
	int soType = tasksQueue->getSOType();
	const uchar* chunkBegin = tasksQueue->getChunkBegin();
	TripleBitQueryGraph::OpType lastOpType;

	while ((chunkTask = tasksQueue->Dequeue()) != NULL) {
		lastOpType = chunkTask->operationType;
		switch (chunkTask->operationType) {
		case TripleBitQueryGraph::QUERY:
			//executeChunkTaskQuery(chunkTask, chunkID, chunkBegin, xyType);
			break;
		case TripleBitQueryGraph::INSERT_DATA:
			executeChunkTaskInsertData(chunkTask, chunkID, chunkBegin, soType);
			chunkTask->indexForTT->completeOneTriple();
			break;
		case TripleBitQueryGraph::DELETE_DATA:
			executeChunkTaskDeleteData(chunkTask, chunkID, chunkBegin, soType);
			chunkTask->indexForTT->completeOneTriple();
			break;
		case TripleBitQueryGraph::DELETE_CLAUSE:
			executeChunkTaskDeleteClause(chunkTask, chunkID, chunkBegin,
					soType);
			chunkTask->indexForTT->completeOneTriple();
			break;
		case TripleBitQueryGraph::UPDATE:
			//executeChunkTaskUpdate(chunkTask, chunkID, chunkBegin, xyType, soType);
			break;
		}
	}
	if (lastOpType == TripleBitQueryGraph::INSERT_DATA) {
		endupdate();
	}
}

void PartitionMaster::executeChunkTaskInsertData(ChunkTask *chunkTask,
		const ID chunkID, const uchar *startPtr, const bool soType) {
	//chunkTask->indexForTT->completeOneTriple();
	xChunkTempBuffer[soType][chunkID]->insertTriple(chunkTask->Triple.subjectID,
			chunkTask->Triple.object, chunkTask->Triple.objType);

	if (xChunkTempBuffer[soType][chunkID]->isFull()) {
		//combine the data in tempbuffer into the source data
		combineTempBufferToSource(xChunkTempBuffer[soType][chunkID], startPtr,
				chunkID, soType);
	}

	//chunkTask->indexForTT->completeOneTriple();
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
	if (chunkID == 0
			&& chunkBegin
					== const_cast<uchar*>(startPtr)
							- sizeof(ChunkManagerMeta)) {
		metaData = (MetaData*) startPtr;
		metaData->usedSpace = currentPtrChunk - const_cast<uchar*>(startPtr);
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

	/*
	 #ifdef MYDEBUG
	 cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
	 #endif
	 */

	assert(buffer != NULL);

	/*
	 #ifdef MYDEBUG
	 ofstream out2;
	 if (soType == ORDERBYS) {
	 out2.open("tempbuffer_sp", ios::app);
	 } else {
	 out2.open("tempbuffer_op", ios::app);
	 }
	 ChunkTriple *temp = buffer->getBuffer();
	 while (temp < buffer->getEnd()) {
	 out2 << chunkID << "," << temp->subjectID << "," << partitionID << ","
	 << temp->object << endl;
	 temp++;
	 }
	 out2.close();
	 #endif
	 */

	buffer->sort(soType);

	/*
	 #ifdef MYDEBUG
	 ofstream out;
	 if (soType == ORDERBYS) {
	 out.open("tempbuffer_sp_sort", ios::app);
	 } else {
	 out.open("tempbuffer_op_sort", ios::app);
	 }
	 temp = buffer->getBuffer();
	 while (temp < buffer->getEnd()) {
	 out << chunkID << "," << temp->subjectID << "," << partitionID << ","
	 << temp->object << endl;
	 temp++;
	 }
	 out << "---" << endl;
	 out.close();
	 #endif
	 */

	buffer->uniqe();

	/*
	 #ifdef MYDEBUG
	 ofstream out1;
	 if (soType == ORDERBYS) {
	 out1.open("tempbuffer_uniqe_sp", ios::app);
	 } else {
	 out1.open("tempbuffer_uniqe_op", ios::app);
	 }
	 ChunkTriple *temp = buffer->getBuffer();
	 while (temp < buffer->getEnd()) {
	 out1 << chunkID << "," << temp->subjectID << "," << partitionID << ","
	 << temp->object << endl;
	 temp++;
	 }
	 out1.close();
	 #endif
	 */

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

	ChunkTriple *tempTriple, *bufferTriple = buffer->getBuffer(),
			*endTempBuffer = buffer->getEnd();
	tempTriple = (ChunkTriple*) malloc(sizeof(ChunkTriple));
	if (tempTriple == NULL) {
		cout << "malloc a ChunkTriple error" << endl;
		free(tempTriple);
		return;
	}
	memset(tempTriple, 0, sizeof(ChunkTriple));
	double max = DBL_MIN, min = DBL_MIN;

	if (currentPtrTemp >= endPtrTemp) {
		tempTriple->subjectID = 0;
		tempTriple->object = 0;
		tempTriple->objType = NONE;
	} else {
		currentPtrTemp = partitionChunkManager[soType]->readXY(currentPtrTemp,
				tempTriple->subjectID, tempTriple->object, tempTriple->objType);
	}
	while (lastPtrTemp < endPtrTemp && bufferTriple < endTempBuffer) {
		//the Ptr not reach the end
		if (tempTriple->subjectID == 0
				|| (tempTriple->subjectID == bufferTriple->subjectID
						&& tempTriple->object == bufferTriple->object
						&& tempTriple->objType == bufferTriple->objType)) {
			//the data is 0 or the data in chunk and tempbuffer are same,so must dismiss it

			readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp, tempPage,
					tempPage2, theOtherPageEmpty, isInTempPage);
			lastPtrTemp = currentPtrTemp;
			if (currentPtrTemp >= endPtrTemp) {
				tempTriple->subjectID = 0;
				tempTriple->object = 0;
				tempTriple->objType = NONE;
			} else {
				currentPtrTemp = partitionChunkManager[soType]->readXY(
						currentPtrTemp, tempTriple->subjectID,
						tempTriple->object, tempTriple->objType);
			}
		} else {
			if ((soType == ORDERBYS
					&& (tempTriple->subjectID < bufferTriple->subjectID
							|| (tempTriple->subjectID == bufferTriple->subjectID
									&& tempTriple->object < bufferTriple->object)
							|| (tempTriple->subjectID == bufferTriple->subjectID
									&& tempTriple->object
											== bufferTriple->object
									&& tempTriple->objType
											< bufferTriple->objType)))
					|| (soType == ORDERBYO
							&& (tempTriple->object < bufferTriple->object
									|| (tempTriple->object
											== bufferTriple->object
											&& tempTriple->subjectID
													< bufferTriple->subjectID)
									|| (tempTriple->object
											== bufferTriple->object
											&& tempTriple->subjectID
													== bufferTriple->subjectID
											&& tempTriple->objType
													< bufferTriple->objType)))) {
				uint len = currentPtrTemp - lastPtrTemp;
				if (currentPtrChunk + len > endPtrChunk) {
					handleEndofChunk(startPtr, chunkBegin, startPtrChunk,
							currentPtrChunk, endPtrChunk, startPtrTemp,
							tempPage, tempPage2, isInTempPage,
							theOtherPageEmpty, min, max, soType, chunkID);
				}
				if (currentPtrChunk == startPtrChunk) {
					min = getChunkMinOrMax(tempTriple, soType);
				}

				memcpy(currentPtrChunk, lastPtrTemp, len);
				max = getChunkMinOrMax(tempTriple, soType) > max ?
						getChunkMinOrMax(tempTriple, soType) : max;
				currentPtrChunk += len;

				//continue read data from tempPage
				readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp,
						tempPage, tempPage2, theOtherPageEmpty, isInTempPage);
				lastPtrTemp = currentPtrTemp;
				if (currentPtrTemp >= endPtrTemp) {
					tempTriple->subjectID = 0;
					tempTriple->object = 0;
					tempTriple->objType = NONE;
				} else {
					currentPtrTemp = partitionChunkManager[soType]->readXY(
							currentPtrTemp, tempTriple->subjectID,
							tempTriple->object, tempTriple->objType);
				}
			} else {
				//insert data read from tempbuffer
				uint len = sizeof(ID) + sizeof(char)
						+ Chunk::getLen(bufferTriple->objType);
				if (currentPtrChunk + len > endPtrChunk) {
					handleEndofChunk(startPtr, chunkBegin, startPtrChunk,
							currentPtrChunk, endPtrChunk, startPtrTemp,
							tempPage, tempPage2, isInTempPage,
							theOtherPageEmpty, min, max, soType, chunkID);
				}
				if (currentPtrChunk == startPtrChunk) {
					min = getChunkMinOrMax(bufferTriple, soType);
				}

				partitionChunkManager[soType]->writeXY(currentPtrChunk,
						bufferTriple->subjectID, bufferTriple->object,
						bufferTriple->objType);
				max = getChunkMinOrMax(bufferTriple, soType) > max ?
						getChunkMinOrMax(bufferTriple, soType) : max;
				currentPtrChunk += len;
				bufferTriple++;
			}
		}
	}

	while (lastPtrTemp < endPtrTemp
			&& (lastPtrTemp >= endPtrTemp
					&& ((MetaData*) startPtrTemp)->NextPageNo)) {
		if (tempTriple->subjectID == 0) {
			readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp, tempPage,
					tempPage2, theOtherPageEmpty, isInTempPage);
			lastPtrTemp = currentPtrTemp;
			if (currentPtrTemp >= endPtrTemp) {
				tempTriple->subjectID = 0;
				tempTriple->object = 0;
				tempTriple->objType = NONE;
			} else {
				currentPtrTemp = partitionChunkManager[soType]->readXY(
						currentPtrTemp, tempTriple->subjectID,
						tempTriple->object, tempTriple->objType);
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
				min = getChunkMinOrMax(tempTriple, soType);
			}

			memcpy(currentPtrChunk, lastPtrTemp, len);
			max = getChunkMinOrMax(tempTriple, soType) > max ?
					getChunkMinOrMax(tempTriple, soType) : max;
			currentPtrChunk += len;

			//continue read data from tempPage
			readIDInTempPage(currentPtrTemp, endPtrTemp, startPtrTemp, tempPage,
					tempPage2, theOtherPageEmpty, isInTempPage);
			lastPtrTemp = currentPtrTemp;
			if (currentPtrTemp >= endPtrTemp) {
				tempTriple->subjectID = 0;
				tempTriple->object = 0;
				tempTriple->objType = NONE;
			} else {
				currentPtrTemp = partitionChunkManager[soType]->readXY(
						currentPtrTemp, tempTriple->subjectID,
						tempTriple->object, tempTriple->objType);
			}
		}
	}

	while (bufferTriple < endTempBuffer) {
		uint len = sizeof(ID) + sizeof(char)
				+ Chunk::getLen(bufferTriple->objType);

		if (currentPtrChunk + len > endPtrChunk) {
			handleEndofChunk(startPtr, chunkBegin, startPtrChunk,
					currentPtrChunk, endPtrChunk, startPtrTemp, tempPage,
					tempPage2, isInTempPage, theOtherPageEmpty, min, max,
					soType, chunkID);
		}
		if (currentPtrChunk == startPtrChunk) {
			min = getChunkMinOrMax(bufferTriple, soType);
		}
		partitionChunkManager[soType]->writeXY(currentPtrChunk,
				bufferTriple->subjectID, bufferTriple->object,
				bufferTriple->objType);

		max = getChunkMinOrMax(bufferTriple, soType) > max ?
				getChunkMinOrMax(bufferTriple, soType) : max;
		currentPtrChunk += len;
		bufferTriple++;
	}

	if (chunkBegin == const_cast<uchar*>(startPtr) - sizeof(ChunkManagerMeta)) {
		MetaData *metaData = (MetaData*) startPtr;
		metaData->usedSpace = currentPtrChunk - const_cast<uchar*>(startPtr);
	} else {
		MetaData *metaData = (MetaData*) chunkBegin;
		metaData->min = min;
		metaData->max = max;
		metaData->usedSpace = currentPtrChunk - chunkBegin;
	}

	partitionChunkManager[soType]->getChunkIndex()->updateChunkMetaData(
			chunkID);

	free(tempTriple);
	tempTriple = NULL;
	free(tempPage);
	tempPage = NULL;
	free(tempPage2);
	tempPage2 = NULL;

	buffer->clear();
}

void PartitionMaster::executeChunkTaskDeleteData(ChunkTask *chunkTask,
		const ID chunkID, const uchar* startPtr, const bool soType) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << endl;
#endif
	ID subjectID = chunkTask->Triple.subjectID;
	ID tempSubjectID;
	double object = chunkTask->Triple.object;
	double tempObject;
	char objType = chunkTask->Triple.objType;
	char tempObjType;
	/*if (soType == ORDERBYS) {
	 ofstream s("dels", ios::app);
	 s << "chunkID," << chunkID << "," << subjectID << "," << partitionID
	 << "," << object << endl;
	 s.close();
	 } else if (soType == ORDERBYO) {
	 ofstream o("delo", ios::app);
	 o << "chunkID," << chunkID << "," << subjectID << "," << partitionID
	 << "," << object << endl;
	 o.close();
	 }*/

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
							&& tempObjType < objType)) {
				continue;
			} else if (tempSubjectID == subjectID && tempObject == object
					&& tempObjType == objType) {
				/*ofstream finds("finds", ios::app);
				 finds << "chunkID," << chunkID << "," << subjectID << ","
				 << partitionID << "," << object << endl;
				 finds.close();*/
				temp = partitionChunkManager[soType]->deleteTriple(temp,
						objType);
				partitionChunkManager[soType]->tripleCountDecrease();
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
								&& tempObjType < objType)) {
					continue;
				} else if (tempSubjectID == subjectID && tempObject == object
						&& tempObjType == objType) {
					temp = partitionChunkManager[soType]->deleteTriple(temp,
							objType);
					partitionChunkManager[soType]->tripleCountDecrease();
					return;
				} else {
					return;
				}
			}
		}
	} else if (soType == ORDERBYO) {
		if (subjectID == 0) { //删除所有记录，主要用于基于模式删除
			while (reader < limit) {
				reader =
						(const uchar*) partitionChunkManager[soType]->deleteTriple(
								const_cast<uchar*>(reader));
				partitionChunkManager[soType]->tripleCountDecrease();
			}
			while (metaData->NextPageNo) {
				chunkBegin =
						reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
								+ MemoryBuffer::pagesize * metaData->NextPageNo;
				metaData = (MetaData*) chunkBegin;
				reader = chunkBegin + sizeof(MetaData);
				limit = chunkBegin + metaData->usedSpace;
				while (reader < limit) {
					reader =
							(const uchar*) partitionChunkManager[soType]->deleteTriple(
									const_cast<uchar*>(reader));
					partitionChunkManager[soType]->tripleCountDecrease();
				}
			}
		} else {
			while (reader < limit) {
				temp = const_cast<uchar*>(reader);
				reader = partitionChunkManager[soType]->readXY(reader,
						tempSubjectID, tempObject, tempObjType);
				if (tempObject < object
						|| (tempObject == object && tempSubjectID < subjectID)
						|| (tempObject == object && tempSubjectID == subjectID
								&& tempObjType < objType)) {
					continue;
				} else if (tempObject == object && tempObjType == objType
						&& tempSubjectID == subjectID) {
					/*ofstream findo("findo", ios::app);
					 findo << "chunkID," << chunkID << "," << subjectID << ","
					 << partitionID << "," << object << endl;
					 findo.close();*/
					temp = partitionChunkManager[soType]->deleteTriple(temp,
							objType);
					partitionChunkManager[soType]->tripleCountDecrease();
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
							|| (tempObject == object
									&& tempSubjectID < subjectID)
							|| (tempObject == object
									&& tempSubjectID == subjectID
									&& tempObjType < objType)) {
						continue;
					} else if (tempObject == object && tempObjType == objType
							&& tempSubjectID == subjectID) {
						temp = partitionChunkManager[soType]->deleteTriple(temp,
								objType);
						partitionChunkManager[soType]->tripleCountDecrease();
						return;
					} else {
						return;
					}
				}
			}
		}
	}

}

void PartitionMaster::deleteDataForDeleteClause(MidResultBuffer *buffer,
		const bool soType, const bool constSubject, const ID subjectID,
		const double object, const char objType) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << endl;
#endif
	int chunkID;
	shared_ptr<subTaskPackage> taskPackage(new subTaskPackage);

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_DATA;
	TripleNode::Op scanType = TripleNode::NOOP;
	if (soType == ORDERBYS) {
		if (constSubject) { //subject是常量，仅删除对应的object
			MidResultBuffer::SignalO* objects = buffer->getObjectBuffer();
			shared_ptr<IndexForTT> indexForTT(
					new IndexForTT(buffer->getUsedSize()));
			cout << "size: " << buffer->getUsedSize() << endl;
			for (size_t i = 0; i < buffer->getUsedSize(); ++i) {
				chunkID =
						partitionChunkManager[ORDERBYO]->getChunkIndex()->searchChunk(
								objects[i].object, subjectID);
				ChunkTask *chunkTask = new ChunkTask(operationType, subjectID,
						objects[i].object, objects[i].objType, scanType,
						taskPackage, indexForTT);
				xChunkQueue[ORDERBYO][chunkID]->EnQueue(chunkTask);
			}
			indexForTT->wait();
		} else { //subject是未知量，删除所有subject与object
			size_t chunkCount = 0, chunkIDMin = 0, chunkIDMax = 0;
			chunkIDMax =
					partitionChunkManager[ORDERBYO]->getChunkIndex()->getTableSize()
							- 1;
			chunkCount = chunkIDMax - chunkIDMin + 1;

			shared_ptr<SubTaskPackageForDelete> taskPackage(
					new SubTaskPackageForDelete());
			if (chunkCount != 0) {
				shared_ptr<IndexForTT> indexForTT(new IndexForTT(chunkCount));
				for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax;
						offsetID++) {
					ChunkTask *chunkTask = new ChunkTask(operationType, 0,
							object, objType, scanType, taskPackage, indexForTT); //subjectID为0表示删除所有记录
					xChunkQueue[ORDERBYO][offsetID]->EnQueue(chunkTask);
				}
				indexForTT->wait();
			}
		}

	} else if (soType == ORDERBYO) {
		ID* subejctIDs = buffer->getSignalIDBuffer();
		cout << __FUNCTION__ << " buffer->getUsedSize(): " << buffer->getUsedSize() << endl;
		shared_ptr<IndexForTT> indexForTT(
				new IndexForTT(buffer->getUsedSize()));
		for (size_t i = 0; i < buffer->getUsedSize(); ++i) {
			chunkID =
					partitionChunkManager[ORDERBYS]->getChunkIndex()->searchChunk(
							subejctIDs[i], object);
			ChunkTask *chunkTask = new ChunkTask(operationType, subejctIDs[i],
					object, objType, scanType, taskPackage, indexForTT);
			xChunkQueue[ORDERBYS][chunkID]->EnQueue(chunkTask);
		}
		indexForTT->wait();
	}
	delete buffer;
}

void PartitionMaster::executeChunkTaskDeleteClause(ChunkTask *chunkTask,
		const ID chunkID, const uchar *startPtr, const bool soType) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << endl;
#endif
	ID subjectID = chunkTask->Triple.subjectID;
	double object = chunkTask->Triple.object;
	char objType = chunkTask->Triple.objType;

	ID tempSubjectID;
	double tempObject;
	char tempObjType;
	const uchar *reader, *limit, *chunkBegin = startPtr;
	uchar *temp;

	MetaData *metaData = (MetaData*) chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	if (!chunkTask->taskPackageForDelete->constSubject
			&& !chunkTask->taskPackageForDelete->constObject) { //subject未知，即为已知predicate，删除subject、object
		while (reader < limit) {
			reader = (const uchar*) partitionChunkManager[soType]->deleteTriple(
					const_cast<uchar*>(reader), tempObjType);
			partitionChunkManager[soType]->tripleCountDecrease();
			continue;
		}
		while (metaData->NextPageNo) {
			chunkBegin =
					reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							+ metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			reader = chunkBegin + sizeof(MetaData);
			limit = chunkBegin + metaData->usedSpace;
			while (reader < limit) {
				reader =
						(const uchar*) partitionChunkManager[soType]->deleteTriple(
								const_cast<uchar*>(reader), tempObjType);
				partitionChunkManager[soType]->tripleCountDecrease();
				continue;
			}
		}
		return;
	}

	MidResultBuffer* midResultBuffer;
	if (soType == ORDERBYS) {
		midResultBuffer = new MidResultBuffer(MidResultBuffer::OBJECT);
	} else if (soType == ORDERBYO) {
		midResultBuffer = new MidResultBuffer(MidResultBuffer::SIGNALID);
	}
	while (reader < limit) {
		temp = const_cast<uchar*>(reader);
		reader = partitionChunkManager[soType]->readXY(reader, tempSubjectID,
				tempObject, tempObjType);
		if (soType == ORDERBYS) {
			//已知predicate、subject，删除object
			if (tempSubjectID < subjectID) {
				continue;
			} else if (tempSubjectID == subjectID) {
				/*cout << subjectID << "," << partitionID << "," << object << "\t";
				 cout << tempSubjectID << "," << partitionID << "," << tempObject << endl;*/
				if (chunkTask->taskPackageForDelete->constObject) {
					if (tempObject < object
							|| (tempObject == object && tempObjType < objType)) {
						continue;
					} else if (tempObject == object && tempObjType == objType) {
						midResultBuffer->insertObject(tempObject, tempObjType);
						temp = partitionChunkManager[soType]->deleteTriple(temp,
								tempObjType);
						partitionChunkManager[soType]->tripleCountDecrease();
						goto END;
					} else {
						delete midResultBuffer;
						midResultBuffer = NULL;
						return;
					}
				}
				midResultBuffer->insertObject(tempObject, tempObjType);
				cout << tempSubjectID << "," << partitionID << "," << tempObject
						<< endl;
				temp = partitionChunkManager[soType]->deleteTriple(temp,
						tempObjType);
				partitionChunkManager[soType]->tripleCountDecrease();
			} else {
				cout << "midResultBuffer->getUsedSize(): "
						<< midResultBuffer->getUsedSize() << endl;
				if (midResultBuffer->getUsedSize() > 0) {
					goto END;
				}
				delete midResultBuffer;
				midResultBuffer = NULL;
				return;
			}
		} else if (soType == ORDERBYO) {
			if (tempObject < object
					|| (tempObject == object && tempObjType < objType)) {
				continue;
			} else if (tempObject == object && tempObjType == objType) {
				midResultBuffer->insertSIGNALID(tempSubjectID);
				temp = partitionChunkManager[soType]->deleteTriple(temp,
						tempObjType);
				partitionChunkManager[soType]->tripleCountDecrease();
			} else {
				cout << "midResultBuffer->getUsedSize(): "
						<< midResultBuffer->getUsedSize() << endl;
				if (midResultBuffer->getUsedSize() > 0) {
					goto END;
				}
				delete midResultBuffer;
				midResultBuffer = NULL;
				return;
			}
		}
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
			reader = partitionChunkManager[soType]->readXY(reader,
					tempSubjectID, tempObject, tempObjType);
			if (soType == ORDERBYS) {
				//已知predicate、subject，删除object
				if (tempSubjectID < subjectID) {
					continue;
				} else if (tempSubjectID == subjectID) {
					if (chunkTask->taskPackageForDelete->constObject) {
						if (tempObject < object
								|| (tempObject == object
										&& tempObjType < objType)) {
							continue;
						} else if (tempObject == object
								&& tempObjType == objType) {
							midResultBuffer->insertObject(tempObject,
									tempObjType);
							temp = partitionChunkManager[soType]->deleteTriple(
									temp, tempObjType);
							partitionChunkManager[soType]->tripleCountDecrease();
							goto END;
						} else {
							delete midResultBuffer;
							midResultBuffer = NULL;
							return;
						}
					}
					midResultBuffer->insertObject(tempObject, tempObjType);
					temp = partitionChunkManager[soType]->deleteTriple(temp,
							tempObjType);
					partitionChunkManager[soType]->tripleCountDecrease();
				} else {
					if (midResultBuffer->getUsedSize() > 0) {
						goto END;
					}
					delete midResultBuffer;
					midResultBuffer = NULL;
					return;

				}
			} else if (soType == ORDERBYO) {
				if (tempObject < object
						|| (tempObject == object && tempObjType < objType)) {
					continue;
				} else if (tempObject == object && tempObjType == objType) {
					midResultBuffer->insertSIGNALID(tempSubjectID);
					temp = partitionChunkManager[soType]->deleteTriple(temp,
							tempObjType);
					partitionChunkManager[soType]->tripleCountDecrease();
				} else {
					if (midResultBuffer->getUsedSize() > 0) {
						goto END;
					}
					delete midResultBuffer;
					midResultBuffer = NULL;
					return;
				}
			}
		}
	}

	END: if (chunkTask->taskPackageForDelete->completeSubTask(chunkID,
			midResultBuffer)) {
		cout << "midResultBuffer->getUsedSize(): " << midResultBuffer->getUsedSize();
		MidResultBuffer *buffer =
				chunkTask->taskPackageForDelete->getTaskResult();
		deleteDataForDeleteClause(buffer, soType,
				chunkTask->taskPackageForDelete->constSubject, subjectID,
				object, objType);
		cout << 22 << endl;
	}

	midResultBuffer = NULL;
}
/*
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
 }*/

void PartitionMaster::findObjectByPredicateAndSubject(const ID subjectID,
		MidResultBuffer *midResultBuffer, const double min, const double max,
		const uchar* startPtr) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
	cout << subjectID << "\t" << min << "\t" << max << "\t" << endl;
#endif

	if (min == DBL_MIN && max == DBL_MAX) {
		findObjectByPredicateAndSubject(subjectID, midResultBuffer, startPtr);
		return;
	}

	ID tmpSubjectID;
	double tmpObject;
	char tmpObjType;
	const uchar* limit, *reader, *chunkBegin = startPtr;

	MetaData *metaData = (MetaData*) chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;
	while (reader < limit) {
		reader = partitionChunkManager[ORDERBYS]->readXY(reader, tmpSubjectID,
				tmpObject, tmpObjType);
		if (tmpSubjectID < subjectID)
			continue;
		else if (tmpSubjectID == subjectID) {
			if (tmpObject < min)
				continue;
			else if (tmpObject <= max) { //注意浮点数等号比较，需修改
				midResultBuffer->insertObject(tmpObject, tmpObjType);
			} else {
				return;
			}

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
			reader = partitionChunkManager[ORDERBYS]->readXY(reader,
					tmpSubjectID, tmpObject, tmpObjType);
			if (tmpSubjectID < subjectID)
				continue;
			else if (tmpSubjectID == subjectID) {
				if (tmpObjType < min)
					continue;
				else if (tmpObjType <= max) {
					midResultBuffer->insertObject(tmpObject, tmpObjType);
				} else {
					return;
				}
			} else {
				return;
			}
		}
	}
}

void PartitionMaster::findObjectByPredicateAndSubject(const ID subjectID,
		MidResultBuffer *midResultBuffer, const uchar *startPtr) {
	ID tmpSubjectID;
	double tmpObject;
	char tmpObjType;
	const uchar *reader, *limit, *chunkBegin = startPtr;

	MetaData* metaData = (MetaData*) chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		reader = partitionChunkManager[ORDERBYS]->readXY(reader, tmpSubjectID,
				tmpObject, tmpObjType);
		if (tmpSubjectID < subjectID)
			continue;
		else if (tmpSubjectID == subjectID)
			midResultBuffer->insertObject(tmpObject, tmpObjType);
		else
			return;
	}
	while (metaData->NextPageNo) {
		chunkBegin =
				reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
						+ MemoryBuffer::pagesize * metaData->NextPageNo;
		metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;

		while (reader < limit) {
			reader = partitionChunkManager[ORDERBYS]->readXY(reader,
					tmpSubjectID, tmpObject, tmpObjType);
			if (tmpSubjectID < subjectID)
				continue;
			else if (tmpSubjectID == subjectID)
				midResultBuffer->insertObject(tmpObject, tmpObjType);
			else
				return;
		}
	}
}

void PartitionMaster::findSubjectIDByPredicateAndObject(const double object,
		const char objType, MidResultBuffer *midResultBuffer, const ID minID,
		const ID maxID, const uchar* startPtr) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	if (minID == 0 && maxID == UINT_MAX) {
		findSubjectIDByPredicateAndObject(object, objType, midResultBuffer,
				startPtr);
		return;
	}

	ID tmpSubjectID;
	double tmpObject;
	char tmpObjType;
	const uchar* limit, *reader, *chunkBegin = startPtr;

	MetaData *metaData = (MetaData*) chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;
	while (reader < limit) {
		reader = partitionChunkManager[ORDERBYO]->readXY(reader, tmpSubjectID,
				tmpObject, tmpObjType);
		if (tmpObject < object || (tmpObject == object && tmpObjType < objType))
			continue;
		else if (tmpObject == object && tmpObjType == objType) {
			if (tmpSubjectID < minID)
				continue;
			else if (tmpSubjectID <= maxID) {
				midResultBuffer->insertSIGNALID(tmpSubjectID);
			} else {
				return;
			}

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
			reader = partitionChunkManager[ORDERBYO]->readXY(reader,
					tmpSubjectID, tmpObject, tmpObjType);
			if (tmpObject < object
					|| (tmpObject == object && tmpObjType < objType))
				continue;
			else if (tmpObject == object && tmpObjType == objType) {
				if (tmpSubjectID < minID)
					continue;
				else if (tmpSubjectID <= maxID) {
					midResultBuffer->insertSIGNALID(tmpSubjectID);
				} else {
					return;
				}
			} else {
				return;
			}
		}
	}

}

void PartitionMaster::findSubjectIDByPredicateAndObject(const double object,
		const char objType, MidResultBuffer *midResultBuffer,
		const uchar *startPtr) {
	ID tmpSubjectID;
	double tmpObject;
	char tmpObjType;
	const uchar *reader, *limit, *chunkBegin = startPtr;

	MetaData* metaData = (MetaData*) chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		reader = partitionChunkManager[ORDERBYO]->readXY(reader, tmpSubjectID,
				tmpObject, tmpObjType);
		if (tmpObject < object || (tmpObject == object && tmpObjType < objType))
			continue;
		else if (tmpObject == object && tmpObjType == objType)
			midResultBuffer->insertSIGNALID(tmpSubjectID);
		else
			return;
	}
	while (metaData->NextPageNo) {
		chunkBegin =
				reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
						+ MemoryBuffer::pagesize * metaData->NextPageNo;
		metaData = (MetaData*) chunkBegin;
		reader = chunkBegin + sizeof(MetaData);
		limit = chunkBegin + metaData->usedSpace;

		while (reader < limit) {
			reader = partitionChunkManager[ORDERBYO]->readXY(reader,
					tmpSubjectID, tmpObject, tmpObjType);
			if (tmpObject < object
					|| (tmpObject == object && tmpObjType < objType))
				continue;
			else if (tmpObject == object && tmpObjType == objType)
				midResultBuffer->insertSIGNALID(tmpSubjectID);
			else
				return;
		}
	}
}

/*void PartitionMaster::findObjectIDAndSubjectIDByPredicate(EntityIDBuffer *retBuffer, const ID minID, const ID maxID, const uchar *startPtr,
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
