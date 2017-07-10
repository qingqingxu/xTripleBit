/*
 * Tasks.h
 *
 *  Created on: 2014-3-6
 *      Author: root
 */

#ifndef TASKS_H_
#define TASKS_H_

#include "Tools.h"
#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "subTaskPackage.h"
#include "IndexForTT.h"

class SubTrans : private Uncopyable{
public:
	struct timeval transTime;
	ID sourceWorkerID;
	ID minID;
	ID maxID;
	TripleBitQueryGraph::OpType operationType;
	size_t tripleNumber;
	TripleNode triple;
	shared_ptr<IndexForTT> indexForTT;

	SubTrans(timeval &transtime, ID sWorkerID, ID miID, ID maID,
			TripleBitQueryGraph::OpType &opType, size_t triNumber,
			TripleNode &trip, shared_ptr<IndexForTT> index_forTT) :
			transTime(transtime), sourceWorkerID(sWorkerID), minID(miID), maxID(
					maID), operationType(opType), tripleNumber(triNumber), triple(
					trip) , indexForTT(index_forTT){
	}
};

class ChunkTask: private Uncopyable{
public:
	TripleBitQueryGraph::OpType operationType;
	ChunkTriple Triple;
	shared_ptr<subTaskPackage> taskPackage;
	shared_ptr<IndexForTT> indexForTT;

	ChunkTask(TripleBitQueryGraph::OpType opType, ID subjectID, double object, char objType, TripleNode::Op operation, shared_ptr<subTaskPackage> task_Package, shared_ptr<IndexForTT> index_ForTT):
		operationType(opType), Triple({subjectID, object, objType, operation}), taskPackage(task_Package), indexForTT(index_ForTT){
	}

	~ChunkTask(){}
};

#endif /* TASKS_H_ */
