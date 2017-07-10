/*
 * TripleBitWorkerQuery.cpp
 *
 *  Created on: 2013-7-1
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitWorkerQuery.h"
#include "TripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "TripleBitQueryGraph.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/IndexForTT.h"
#include "comm/Tasks.h"
#include <algorithm>
#include <math.h>
#include <pthread.h>
#include "util/Timestamp.h"

#define BLOCK_SIZE 1024
//#define PRINT_RESULT
#define COLDCACHE
//#define MYDEBUG

TripleBitWorkerQuery::TripleBitWorkerQuery(TripleBitRepository*& repo, ID workID) {
	tripleBitRepo = repo;
	bitmap = repo->getBitmapBuffer();
	uriTable = repo->getURITable();
	preTable = repo->getPredicateTable();

	workerID = workID; // 1 ~ WORKERNUM

	tasksQueueWP = repo->getTasksQueueWP();
	resultWP = repo->getResultWP();
	tasksQueueWPMutex = repo->getTasksQueueWPMutex();
	partitionNum = repo->getPartitionNum();

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID) {
		tasksQueue[partitionID] = tasksQueueWP[partitionID - 1];
	}

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID) {
		resultBuffer[partitionID] = resultWP[(workerID - 1) * partitionNum + partitionID - 1];
	}
}

TripleBitWorkerQuery::~TripleBitWorkerQuery() {
	EntityIDList.clear();
}

void TripleBitWorkerQuery::releaseBuffer() {
	idTreeBFS.clear();
	leafNode.clear();
	varVec.clear();

	EntityIDListIterType iter = EntityIDList.begin();

	for (; iter != EntityIDList.end(); iter++) {
		if (iter->second != NULL)
			BufferManager::getInstance()->freeBuffer(iter->second);
	}

	BufferManager::getInstance()->reserveBuffer();
	EntityIDList.clear();
}

Status TripleBitWorkerQuery::query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime) {
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;
	this->transactionTime = &transTime;

	switch (_queryGraph->getOpType()) {
	/*case TripleBitQueryGraph::QUERY:
		return excuteQuery();*/
	case TripleBitQueryGraph::INSERT_DATA:
		return excuteInsertData();
	case TripleBitQueryGraph::DELETE_DATA:
		return excuteDeleteData();
/*	case TripleBitQueryGraph::DELETE_CLAUSE:
		return excuteDeleteClause();
	case TripleBitQueryGraph::UPDATE:
		return excuteUpdate();*/
	}
}

/*void TripleBitWorkerQuery::displayAllTriples()
{
	ID predicateCount = preTable->getPredicateNo();
	ID tripleCount = 0;
	for(ID pid = 1;pid < predicateCount;pid++)tripleCount += bitmap->getChunkManager(pid, 0)->getTripleCount();
	cout<<"The total tripleCount is : "<<tripleCount<<endl<<"Do you really want to display all (Y / N) ?"<<endl;
	char displayFlag;
	cin>>displayFlag;

	if((displayFlag == 'y') || (displayFlag == 'Y')){
		EntityIDBuffer *buffer = new EntityIDBuffer();
		string URI1,URI2;
		for(ID pid = 1;pid < predicateCount;pid++){
			buffer->empty();
			entityFinder->findSubjectIDAndObjectIDByPredicate(pid, buffer);
			size_t bufSize = buffer->getSize();
			if(bufSize == 0)continue;
			ID *p = buffer->getBuffer();
			preTable->getPredicateByID(URI1, pid);

			for (size_t i = 0; i < bufSize; i++) {
				uriTable->getURIById(URI2, p[i * 2]);
				cout << URI2 << " ";
				cout << URI1 << " ";
				uriTable->getURIById(URI2, p[i * 2 + 1]);
				cout << URI2 << endl;
			}
		}

		delete buffer;
	}
}

void TripleBitWorkerQuery::onePatternWithThreeVariables()
{
	if(_query->tripleNodes[0].scanOperation == TripleNode::FINDSPO){
		displayAllTriples();
	}else if(_query->tripleNodes[0].scanOperation == TripleNode::FINDSPBYNONE){
		if(_queryGraph->getProjection().front() == _query->tripleNodes[0].subject){
			unsigned predicateCount = preTable->getPredicateNo();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findSubjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				preTable->getPredicateByID(URI2, pid);
				for (size_t i = 0; i < bufSize; i++) {
					uriTable->getURIById(URI1, p[i]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}else{
			unsigned predicateCount = preTable->getPredicateNo();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findSubjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				preTable->getPredicateByID(URI2, pid);
				for (size_t i = 0; i < bufSize; i++) {
					uriTable->getURIById(URI1, p[i]);
					cout<<URI2<<" "<<URI1<<endl;
				}
			}
			delete buffer;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleNode::FINDPOBYNONE){
		if(_queryGraph->getProjection().front() == _query->tripleNodes[0].predicate){
			unsigned predicateCount = preTable->getPredicateNo();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findObjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				URI2 = preTable->getPredicateByID(pid);
				for (size_t i = 0; i < bufSize; i++) {
					UriTable->getURIById(URI1, p[i]);
					cout<<URI2<<" "<<URI1<<endl;
				}
			}
			delete buffer;
		}else{
			unsigned predicateCount = preTable->getPredicateNo();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findObjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				preTable->getPredicateByID(URI2, pid);
				for (size_t i = 0; i < bufSize; i++) {
					uriTable->getURIById(URI1, p[i]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleNode::FINDSOBYNONE){
		if(_queryGraph->getProjection().front() == _query->tripleNodes[0].subject){
			unsigned predicateCount = preTable->getPredicateNo();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findSubjectIDAndObjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				for (size_t i = 0; i < bufSize; i++) {
					uriTable->getURIById(URI1, p[i*2]);
					uriTable->getURIById(URI2, p[i*2+1]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}else{
			unsigned predicateCount = preTable->getPredicateNo();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findObjectIDAndSubjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				for (size_t i = 0; i < bufSize; i++) {
					uriTable->getURIById(URI1, p[i*2]);
					uriTable->getURIById(URI2, p[i*2+1]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleNode::FINDS){
		EntityIDBuffer *buffer = new EntityIDBuffer();
		entityFinder->findSubject(buffer, 0, INT_MAX);
		size_t bufSize = buffer->getSize();
		ID *p = buffer->getBuffer();
		string URI;
		for(size_t i = 0;i < bufSize;i++){
			uriTable->getURIById(URI, p[i]);
			cout<<URI<<endl;
		}
		delete buffer;
	}else if(_query->tripleNodes[0].scanOperation == TripleNode::FINDP){
		unsigned predicateCount = preTable->getPredicateNo();
		string URI;
		for(ID pid = 1;pid < predicateCount;pid++){
			preTable->getPredicateByID(URI, pid);
			cout<<URI<<endl;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleNode::FINDO){
		EntityIDBuffer *buffer = new EntityIDBuffer();
		entityFinder->findObject(buffer, 0, INT_MAX);
		size_t bufSize = buffer->getSize();
		ID *p = buffer->getBuffer();
		string URI;
		for(size_t i = 0;i < bufSize;i++){
			uriTable->getURIById(URI, p[i]);
			cout<<URI<<endl;
		}
		delete buffer;
	}
}*/


/*
Status TripleBitWorkerQuery::excuteQuery() {
	if(_query->tripleNodes.size() == 1 && _query->joinVariables.size() == 3){
			//the query has only one pattern with three variables
			onePatternWithThreeVariables();
		}
	else if (_query->joinVariables.size() == 1) {
//#ifdef MYDEBUG
		cout << "execute singleVariableJoin" << endl;
//#endif
		singleVariableJoin();
	} else {
		if (_query->joinGraph == TripleBitQueryGraph::ACYCLIC) {
//#ifdef MYDEBUG
			cout << "execute asyclicJoin" << endl;
//#endif
			acyclicJoin();
		} else if (_query->joinGraph == TripleBitQueryGraph::CYCLIC) {
//#ifdef MYDEBUG
			cout << "execute cyclicJoin" << endl;
//#endif
			cyclicJoin();
		}
	}
	return OK;
}
*/

void TripleBitWorkerQuery::tasksEnQueue(ID partitionID, SubTrans *subTrans) {
	if (tasksQueue[partitionID]->Queue_Empty()) {
		tasksQueue[partitionID]->EnQueue(subTrans);
		ThreadPool::getPartitionPool().addTask(boost::bind(&PartitionMaster::Work, tripleBitRepo->getPartitionMaster(partitionID)));
	} else {
		tasksQueue[partitionID]->EnQueue(subTrans);
	}
}

Status TripleBitWorkerQuery::excuteInsertData() {
	size_t tripleSize = _query->tripleNodes.size();
	//shared_ptr<IndexForTT> indexForTT(new IndexForTT(tripleSize * 2));

	classifyTripleNode();

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::INSERT_DATA;

	map<ID, set<TripleNode*> >::iterator iter = tripleNodeMap.begin();
	for (; iter != tripleNodeMap.end(); ++iter) {
		size_t tripleNodeSize = iter->second.size();
		ID partitionID = iter->first;
		set<TripleNode*>::iterator tripleNodeIter = iter->second.begin();

		tasksQueueWPMutex[partitionID - 1]->lock();
		for (; tripleNodeIter != iter->second.end(); ++tripleNodeIter) {
			SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, tripleNodeSize, *(*tripleNodeIter)/*, indexForTT*/);
			tasksEnQueue(partitionID, subTrans);
		}
		tasksQueueWPMutex[partitionID - 1]->unlock();
	}

	//indexForTT->wait();
	return OK;
}

Status TripleBitWorkerQuery::excuteDeleteData() {
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);

	classifyTripleNode();

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_DATA;

	map<ID, set<TripleNode*> >::iterator iter = tripleNodeMap.begin();
	for (; iter != tripleNodeMap.end(); ++iter) {
		size_t tripleNodeSize = iter->second.size();
		ID partitionID = iter->first;
		set<TripleNode*>::iterator tripleNodeIter = iter->second.begin();

		tasksQueueWPMutex[partitionID - 1]->lock();
		for (; tripleNodeIter != iter->second.end(); ++tripleNodeIter) {
			SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, tripleNodeSize, *(*tripleNodeIter), indexForTT);
			tasksEnQueue(partitionID, subTrans);
		}
		tasksQueueWPMutex[partitionID - 1]->unlock();
	}

	return OK;
}

/*
Status TripleBitWorkerQuery::excuteDeleteClause() {
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);

	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	ID partitionID = iter->predicate;
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_CLAUSE;

	SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, 1, *iter, indexForTT);
	tasksEnQueue(partitionID, subTrans);

	return OK;
}

Status TripleBitWorkerQuery::excuteUpdate() {
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);

	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	ID partitionID = iter->predicate;
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::UPDATE;

	SubTrans *subTrans1 = new SubTrans(*transactionTime, workerID, 0, 0, operationType, 2, *iter, indexForTT);
	SubTrans *subTrans2 = new SubTrans(*transactionTime, workerID, 0, 0, operationType, 2, *++iter, indexForTT);

	tasksQueueWPMutex[partitionID - 1]->lock();
	tasksEnQueue(partitionID, subTrans1);
	tasksEnQueue(partitionID, subTrans2);
	tasksQueueWPMutex[partitionID - 1]->unlock();

	return OK;
}*/

void TripleBitWorkerQuery::classifyTripleNode() {
	tripleNodeMap.clear();
	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();

	for (; iter != _query->tripleNodes.end(); ++iter) {
		tripleNodeMap[iter->predicateID].insert(&(*iter));
	}
}
/*
static void generateProjectionBitVector(uint& bitv, std::vector<ID>& project) {
	bitv = 0;
	for (size_t i = 0; i != project.size(); ++i) {
		bitv |= 1 << project[i];
	}
}

static void generateTripleNodeBitVector(uint& bitv, TripleNode& node) {
	bitv = 0;
	if (!node.constSubject)
		bitv |= (1 << node.subject);
	if (!node.constPredicate)
		bitv |= (1 << node.predicate);
	if (!node.constObject)
		bitv |= (1 << node.object);
}

static size_t countOneBits(uint bitv) {
	size_t count = 0;
	while (bitv) {
		bitv = bitv & (bitv - 1);
		count++;
	}
	return count;
}

static ID bitVtoID(uint bitv) {
	uint mask = 0x1;
	ID count = 0;
	while (true) {
		if ((mask & bitv) == mask)
			break;
		bitv = bitv >> 1;
		count++;
	}
	return count;
}

static int insertVarID(ID key, std::vector<ID>& idvec, TripleNode& node, ID& sortID) {
	int ret = 0;
	switch (node.scanOperation) {
	case TripleNode::FINDO:
	case TripleNode::FINDOBYP:
	case TripleNode::FINDOBYS:
	case TripleNode::FINDOBYSP:
		if (key != node.object)
			idvec.push_back(node.object);
		sortID = node.object;
		break;
	case TripleNode::FINDOPBYS:
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleNode::FINDOSBYP:
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleNode::FINDP:
	case TripleNode::FINDPBYO:
	case TripleNode::FINDPBYS:
	case TripleNode::FINDPBYSO:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		sortID = node.predicate;
		break;
	case TripleNode::FINDPOBYS:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleNode::FINDPSBYO:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleNode::FINDS:
	case TripleNode::FINDSBYO:
	case TripleNode::FINDSBYP:
	case TripleNode::FINDSBYPO:
		if (key != node.subject)
			idvec.push_back(node.subject);
		sortID = node.subject;
		break;
	case TripleNode::FINDSOBYP:
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleNode::FINDSPBYO:
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleNode::NOOP:
		break;
	}

	return ret;
}

static void generateResultPos(std::vector<ID>& idVec, std::vector<ID>& projection, std::vector<int>& resultPos) {
	resultPos.clear();

	std::vector<ID>::iterator iter;
	for (size_t i = 0; i != projection.size(); ++i) {
		iter = find(idVec.begin(), idVec.end(), projection[i]);
		resultPos.push_back(iter - idVec.begin());
	}
}

static void generateVerifyPos(std::vector<ID>& idVec, std::vector<int>& verifyPos) {
	verifyPos.clear();
	size_t i, j;
	size_t size = idVec.size();
	for (i = 0; i != size; ++i) {
		for (j = i + 1; j != size; j++) {
			if (idVec[i] == idVec[j]) {
				verifyPos.push_back(i);
				verifyPos.push_back(j);
			}
		}
	}
}

void PrintEntityBufferID(EntityIDBuffer *buffer) {
	size_t size = buffer->getSize();
	ID* resultBuffer = buffer->getBuffer();

	for (size_t i = 0; i < size; ++i) {
		cout << "ID:" << resultBuffer[i] << endl;
	}
	cout << "ResultBuffer Size: " << size << endl;
}

void printSomethingWorker(EntityIDBuffer* buffer) {
	size_t size = buffer->getSize();
	int count = 0;
	ID* resultBuffer = buffer->getBuffer();

	int IDCount = buffer->getIDCount();

	if (IDCount == 1) {
		for (size_t i = 1; i < size; ++i) {
			if (resultBuffer[i - 1] > resultBuffer[i])
				cout << "Buffer Error" << endl;
		}
	} else if (IDCount == 2) {
		for (size_t i = 3; i < size; i = i + 2) {
			if (resultBuffer[i - 3] > resultBuffer[i - 1])
				cout << "Buffer Error i-3,i-1" << endl;
			else if (resultBuffer[i - 3] == resultBuffer[i - 1]) {
				if (resultBuffer[i - 2] > resultBuffer[i])
					cout << "Buffer Error i-2,i" << endl;
			}
		}
	}

	for (size_t i = 0; i < size; ++i) {
		cout << "ID:" << resultBuffer[i] << ' ';
		count++;
		if (count % 20 == 0)
			cout << endl;
	}
	cout << "ResultBuffer Size: " << size << endl;
	cout << endl;
}

Status TripleBitWorkerQuery::singleVariableJoin() {
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node = NULL;
	EntityIDBuffer* buffer = NULL;
	TripleNode* triple = NULL;

	//TODO Initialize the first query pattern's triple of the pattern group which has the same variable
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();

	getTripleNodeByID(triple, nodePatternIter->first);
	buffer = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
	if (buffer->getSize() == 0) {
#ifdef PRINT_RESULT
		cout << "empty result" << endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
		return OK;
	}

	EntityIDList[nodePatternIter->first] = buffer;
	nodePatternIter++;

	ResultIDBuffer* tempBuffer;
	ID minID, maxID;

	if (_queryGraph->getProjection().size() == 1) {
		for (; nodePatternIter != node->appear_tpnodes.end(); ++nodePatternIter) {
			buffer->getMinMax(minID, maxID);
			getTripleNodeByID(triple, nodePatternIter->first);
			tempBuffer = findEntityIDByTriple(triple, minID, maxID);
			mergeJoin.Join(buffer, tempBuffer, 1, 1, false);
			if (buffer->getSize() == 0) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	} else {
		for (; nodePatternIter != node->appear_tpnodes.end(); nodePatternIter++) {
			buffer->getMinMax(minID, maxID);
			getTripleNodeByID(triple, nodePatternIter->first);
			tempBuffer = findEntityIDByTriple(triple, minID, maxID);
			mergeJoin.Join(buffer, tempBuffer, 1, 1, true);
			if (buffer->getSize() == 0) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
			EntityIDList[nodePatternIter->first] = tempBuffer->getEntityIDBuffer();
		}
	}

	//TODO materialization the result;
	size_t i;
	size_t size = buffer->getSize();

	std::string URI;
	ID* p = buffer->getBuffer();

	size_t projectNo = _queryGraph->getProjection().size();
#ifndef PRINT_RESULT
	char temp[2];
	sprintf(temp, "%d", projectNo);
	resultPtr->push_back(temp);
#endif

	resultPtr->clear();
	if (projectNo == 1) {
		for (i = 0; i < size; ++i) {
			if (uriTable->getURIById(URI, p[i]) == OK) {
#ifdef PRINT_RESULT
				cout << URI << endl;
#else
				resultPtr->push_back(URI);
#endif
			} else {
#ifdef PRINT_RESULT
				cout << p[i] << " " << "not found" << endl;
#else
				resultPtr->push_back("NULL");
#endif
			}
		}
	} else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		ID sortID;
		int keyp;
		keyPos.clear();

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		for (i = 0; i != _query->tripleNodes.size(); i++) {
			//generate the bit vector of query pattern
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			//get the common bit
			tempBitV = projBitV & nodeBitV;
			if (tempBitV == nodeBitV) {
				//the query pattern which contains two or more variables is better
				if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1))
					continue;
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
				if (countOneBits(resultBitV) == 0) {
					//the first time, last joinKey should be set as UNIT_MAX
					keyp = insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				} else {
					keyp = insertVarID(*joinNodeIter, resultVar, _query->tripleNodes[i], sortID);
					keyPos.push_back(keyp);
				}
				resultBitV |= nodeBitV;
				//the buffer of query pattern is enough
				if (countOneBits(resultBitV) == projectNo)
					break;
			}
		}

		if (projectNo == 2) {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);

			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			ID* ids = buf->getBuffer();
			int IDCount = buf->getIDCount();
			for (i = 0; i < bufsize; i++) {
				for (int j = 0; j < IDCount; j++) {
					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
#ifdef PRINT_RESULT
						cout << URI << " ";
#else
						resultPtr->push_back(URI);
#endif
					} else
						cout << "not found" << endl;
				}
#ifdef PRINT_RESULT
				cout << endl;
#endif
			}
		} else {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
			needselect = false;

			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.clear();
			bufPreIndexs.resize(bufferlist.size(), 0);
			ID key;
			int IDCount = buf->getIDCount();
			ID* ids = buf->getBuffer();
			int sortKey = buf->getSortKey();
			for (i = 0; i != bufsize; i++) {
				resultVec.resize(0);
				key = ids[i * IDCount + sortKey];
				for (int j = 0; j < IDCount; j++) {
					resultVec.push_back(ids[i * IDCount + j]);
				}

				bool ret = getResult(key, bufferlist, 1);
				if (ret == false) {
					while (i < bufsize && ids[i * IDCount + sortKey] == key) {
						i++;
					}
					i--;
				}
			}
		}
	}

	return OK;
}

ResultIDBuffer* TripleBitWorkerQuery::findEntityIDByTriple(TripleNode* triple, ID minID, ID maxID) {
#ifdef PATTERN_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);
	SubTrans *subTrans = new SubTrans(*transactionTime, workerID, minID, maxID, operationType, 1, *triple, indexForTT);
	tasksEnQueue(triple->predicate, subTrans);
	ResultIDBuffer *retBuffer = resultBuffer[triple->predicate]->DeQueue();

#ifdef PATTERN_TIME
	gettimeofday(&end, NULL);
	cout << "find pattern " << triple->tripleNodeID << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
			<< endl;
#endif

	return retBuffer;
}

Status TripleBitWorkerQuery::getTripleNodeByID(TripleNode*& triple, TripleBitQueryGraph::TripleNodeID nodeID) {
	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();

	for (; iter != _query->tripleNodes.end(); iter++) {
		if (iter->tripleNodeID == nodeID) {
			triple = &(*iter);
			return OK;
		}
	}
	return NOT_FOUND;
}

int TripleBitWorkerQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleBitQueryGraph::TripleNodeID tripleID) {
	TripleNode* triple = NULL;
	getTripleNodeByID(triple, tripleID);
	return getVariablePos(id, triple);
}

int TripleBitWorkerQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleNode* triple) {
	int pos = 0;

	switch (triple->scanOperation) {
	case TripleNode::FINDO:
	case TripleNode::FINDOBYP:
	case TripleNode::FINDOBYS:
	case TripleNode::FINDOBYSP:
		pos = 1;
		break;
	case TripleNode::FINDOPBYS:
		if (id == triple->object)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDOSBYP:
		if (id == triple->object)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDP:
	case TripleNode::FINDPBYO:
	case TripleNode::FINDPBYS:
	case TripleNode::FINDPBYSO:
		pos = 1;
		break;
	case TripleNode::FINDPOBYS:
		if (id == triple->predicate)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDPSBYO:
		if (id == triple->predicate)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDS:
	case TripleNode::FINDSBYO:
	case TripleNode::FINDSBYP:
	case TripleNode::FINDSBYPO:
		pos = 1;
		break;
	case TripleNode::FINDSOBYP:
		if (id == triple->subject)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDSPBYO:
		if (id == triple->subject)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::NOOP:
		pos = -1;
		break;
	}
	return pos;
}

Status TripleBitWorkerQuery::getVariableNodeByID(TripleBitQueryGraph::JoinVariableNode*& node, TripleBitQueryGraph::JoinVariableNodeID id) {
	int size = _query->joinVariableNodes.size();

	for (int i = 0; i < size; i++) {
		if (_query->joinVariableNodes[i].value == id) {
			node = &(_query->joinVariableNodes[i]);
			break;
		}
	}

	return OK;
}

bool TripleBitWorkerQuery::nodeIsLeaf(TripleBitQueryGraph::JoinVariableNodeID varID) {
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator iter;
	iter = find(_query->leafNodes.begin(), _query->leafNodes.end(), varID);
	if (iter != _query->leafNodes.end())
		return true;
	else
		return false;
}

int TripleBitWorkerQuery::getVariableCount(TripleNode* node) {
	if (node == NULL) {
		cerr << "Error TripleBitWorkerQuery::getVariableCount" << endl;
		return -1;
	}
	switch (node->scanOperation) {
	case TripleNode::FINDO:
		return 1;
	case TripleNode::FINDOBYP:
		return 1;
	case TripleNode::FINDOBYS:
		return 1;
	case TripleNode::FINDOBYSP:
		return 1;
	case TripleNode::FINDP:
		return 1;
	case TripleNode::FINDPBYO:
		return 1;
	case TripleNode::FINDPBYS:
		return 1;
	case TripleNode::FINDPBYSO:
		return 1;
	case TripleNode::FINDS:
		return 1;
	case TripleNode::FINDSBYO:
		return 1;
	case TripleNode::FINDSBYP:
		return 1;
	case TripleNode::FINDSBYPO:
		return 1;
	case TripleNode::FINDOSBYP:
		return 2;
	case TripleNode::FINDOPBYS:
		return 2;
	case TripleNode::FINDPSBYO:
		return 2;
	case TripleNode::FINDPOBYS:
		return 2;
	case TripleNode::FINDSOBYP:
		return 2;
	case TripleNode::FINDSPBYO:
		return 2;
	case TripleNode::NOOP:
		return -1;
	}

	return -1;
}

int TripleBitWorkerQuery::getVariableCount(TripleBitQueryGraph::TripleNodeID id) {
	TripleNode* triple = NULL;
	getTripleNodeByID(triple, id);
	return getVariableCount(triple);
}

bool TripleBitWorkerQuery::getResult(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	if (buflist_index == bufferlist.size())
		return true;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = bufPreIndexs[buflist_index];
	size_t bufsize = entBuf->getSize();
	while (currentIndex < bufsize && (*entBuf)[currentIndex] < key) {
		currentIndex++;
	}
	bufPreIndexs[buflist_index] = currentIndex;
	if (currentIndex >= bufsize || (*entBuf)[currentIndex] > key)
		return false;

	bool ret;
	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	ret = true;
	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; ++i) {
			if (i != sortKey) {
				resultVec.push_back(buf[currentIndex * IDCount + i]);
			}
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					++currentIndex;
					continue;
				}
			}
			for (size_t j = 0; j != resultPos.size(); j++) {
				uriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		} else {
			ret = getResult(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
			if (ret != true)
				break;
		}

		++currentIndex;
		resultVec.resize(resultVecSize);
	}

	return ret;
}

void TripleBitWorkerQuery::getResult_join(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	if (buflist_index == bufferlist.size())
		return;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = entBuf->getEntityIDPos(key); //bufPreIndexs[buflist_index]
	if (currentIndex == size_t(-1))
		return;
	size_t bufsize = entBuf->getSize();

	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; ++i) {
			if (i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					++currentIndex;
					continue;
				}
			}
			for (size_t j = 0; j != resultPos.size(); ++j) {
				uriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		} else {
			getResult_join(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
		}

		++currentIndex;
		resultVec.resize(resultVecSize);
	}
}

Status TripleBitWorkerQuery::findEntitiesAndJoin(TripleBitQueryGraph::JoinVariableNodeID id,
		vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes, bool firstTime) {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " JoinVariableNodeID: " << id << "\tfirstTime: " << firstTime << endl;
#endif

	size_t minSize; // minimum record size;
	ID minSizeID; //the bufferer No. which contains the minimum records;
	size_t size;
	EntityIDBuffer* buffer;
	map<ID, bool> firstInsertFlag; // flag the first inserted pattern buffer;
	Status s;

	minSize = INT_MAX;
	minSizeID = 0;

	size = EntityIDList.size();

	//TODO get the buffer id which contains minimum candidate result
	EntityIDListIterType iter;
	size_t i;

	if (firstTime == true) {
		for (i = 0; i < tpnodes.size(); ++i) {
			firstInsertFlag[tpnodes[i].first] = false;
			iter = EntityIDList.find(tpnodes[i].first);
			if (iter != EntityIDList.end()) {
				if ((size = iter->second->getSize()) < minSize) {
					minSize = size;
					minSizeID = tpnodes[i].first;
				}
			} else if (getVariableCount(tpnodes[i].first) >= 2) {
				//TODO if the number of variable in a pattern is greater than 1, then allocate a buffer for the pattern
				buffer = NULL;
				EntityIDList[tpnodes[i].first] = buffer;
				firstInsertFlag[tpnodes[i].first] = true;
			}
		}

	} else {
		for (i = 0; i < tpnodes.size(); ++i) {
			firstInsertFlag[tpnodes[i].first] = false;
			if (getVariableCount(tpnodes[i].first) >= 2) {
				if (EntityIDList[tpnodes[i].first]->getSize() < minSize) {
					minSizeID = tpnodes[i].first;
					minSize = EntityIDList[minSizeID]->getSize();
				}
			}
		}
	}

	//if the most selective pattern has not been initialized , then initialize it
	iter = EntityIDList.find(minSizeID);
	if (iter == EntityIDList.end()) {
		TripleNode* triple = NULL;
		getTripleNodeByID(triple, minSizeID);
		EntityIDList[minSizeID] = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
	}

	if (firstTime == true)
		s = findEntitiesAndJoinFirstTime(tpnodes, minSizeID, firstInsertFlag, id);
	else
		s = modifyEntitiesAndJoin(tpnodes, minSizeID, id);

	return s;
}

Status TripleBitWorkerQuery::findEntitiesAndJoinFirstTime(
		vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes, ID tripleID, map<ID, bool>& firstInsertFlag,
		TripleBitQueryGraph::JoinVariableNodeID id) {
	//use merge join operator to do the joins
	EntityIDBuffer* buffer = EntityIDList[tripleID];

	EntityIDBuffer* tempEntity = NULL;
	ResultIDBuffer* tempResult = NULL;
	TripleNode* triple = NULL;
	int joinKey = 0, joinKey2 = 0;
	bool insertFlag;
	size_t i;
	ID tripleNo;
	int varCount;

	ID maxID, minID;

	if (tpnodes.size() == 1) {
		return OK;
	}

	joinKey = this->getVariablePos(id, tripleID);
	buffer->sort(joinKey);

	for (i = 0; i < tpnodes.size(); ++i) {
		tripleNo = tpnodes[i].first;

		if (tripleNo != tripleID) {
			joinKey2 = this->getVariablePos(id, tripleNo);
			insertFlag = firstInsertFlag[tripleNo];
			getTripleNodeByID(triple, tripleNo);
			varCount = this->getVariableCount(triple);

			if (insertFlag == false && varCount == 1) {
				buffer->getMinMax(minID, maxID);
				tempResult = findEntityIDByTriple(triple, minID, maxID);
#ifdef PRINT_BUFFERSIZE
				cout << "case 1" << endl;
				cout << "pattern " << tripleID << " buffer size(before join): " << buffer->getSize() << endl;
#endif
				mergeJoin.Join(buffer, tempResult, joinKey, 1, false);
#ifdef PRINT_BUFFERSIZE
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
#endif
				BufferManager::getInstance()->freeBuffer(tempResult->getEntityIDBuffer());
				delete tempResult;
			} else if (insertFlag == false && varCount == 2) {
				tempEntity = EntityIDList[tripleNo];
				mergeJoin.Join(buffer, tempEntity, joinKey, joinKey2, true);
#ifdef PRINT_BUFFERSIZE
				cout << "case 2" << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size: " << tempEntity->getSize() << endl;
#endif
			} else {
				buffer->getMinMax(minID, maxID);
				tempResult = findEntityIDByTriple(triple, minID, maxID);
#ifdef PRINT_BUFFERSIZE
				cout << "case 3" << endl;
#endif
				mergeJoin.Join(buffer, tempResult, joinKey, joinKey2, true);
#ifdef PRINT_BUFFERSIZE
				cout << "pattern " << tripleNo << " buffer size: " << tempResult->getSize() << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
#endif
				EntityIDList[tripleNo] = tempResult->getEntityIDBuffer();
			}
		}

		if (buffer->getSize() == 0)
			return NULL_RESULT;
	}

	if (buffer->getSize() == 0) {
		return NULL_RESULT;
	}

	return OK;
}

Status TripleBitWorkerQuery::modifyEntitiesAndJoin(vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
		ID tripleID, TripleBitQueryGraph::JoinVariableNodeID id) {
	//use hash join operator to do the joins
	EntityIDBuffer* buffer = EntityIDList[tripleID];
	EntityIDBuffer* temp = NULL;
	TripleNode* triple = NULL;
	int joinKey = 0, joinKey2 = 0;
	size_t i;
	ID tripleNo;
	int varCount;
	bool sizeChanged = false;

	joinKey = this->getVariablePos(id, tripleID);
	buffer->setSortKey(joinKey - 1);
	size_t size = buffer->getSize();

	for (i = 0; i < tpnodes.size(); ++i) {
		tripleNo = tpnodes[i].first;
		this->getTripleNodeByID(triple, tripleNo);
		if (tripleNo != tripleID) {
			varCount = getVariableCount(triple);
			if (varCount == 2) {
				joinKey2 = this->getVariablePos(id, tripleID);
				temp = EntityIDList[tripleNo];
#ifdef PRINT_BUFFERSIZE
				cout << "------------------------------------" << endl;
				cout << "pattern " << tripleNo << " buffer size: " << temp->getSize() << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
#endif
				hashJoin.Join(buffer, temp, joinKey, joinKey2);
#ifdef PRINT_BUFFERSIZE
				cout << "pattern " << tripleNo << " buffer size: " << temp->getSize() << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
#endif
			}
		}
		if (buffer->getSize() == 0) {
			return NULL_RESULT;
		}
	}
	if (size != buffer->getSize() || sizeChanged == true)
		return BUFFER_MODIFIED;
	else
		return OK;
}

Status TripleBitWorkerQuery::acyclicJoin() {
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node = NULL;
	EntityIDBuffer *buffer = NULL;
	TripleNode* triple = NULL;

	//initialize the patterns which are related to the first variable.
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();
	getTripleNodeByID(triple, nodePatternIter->first);
	buffer = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
	EntityIDList[nodePatternIter->first] = buffer;

	buffer = EntityIDList[nodePatternIter->first];
	if (this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT) {
#ifdef PRINT_RESULT
		cout << "empty result" << endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
		return OK;
	}

	joinNodeIter++;
	//iterate the join variables
	for (; joinNodeIter != _query->joinVariables.end(); joinNodeIter++) {
		getVariableNodeByID(node, *joinNodeIter);
		if (node->appear_tpnodes.size() > 1) {
			if (this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	//iterate reverse
	TripleBitQueryGraph::JoinVariableNodeID varID;
	bool isLeafNode;
	size_t size = _query->joinVariables.size();
	size_t i;

	for (i = 0; i < size; ++i) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == true) {
			getVariableNodeByID(node, varID);
			if (this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	//TODO materialize
	size_t projectionSize = _queryGraph->getProjection().size();
	if (projectionSize == 1) {
		uint projBitV, nodeBitV, tempBitV;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		ID sortID;

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		TripleBitQueryGraph::TripleNodeID tid;
		size_t bufsize = UINT_MAX;
		for (i = 0; i != _query->tripleNodes.size(); ++i) {
			if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0)
				continue;
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			tempBitV = projBitV | nodeBitV;
			if (tempBitV == projBitV) {
				//TODO output the result
				if (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1) {
					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
					tid = _query->tripleNodes[i].tripleNodeID;
					break;
				} else {
					if (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSize() < bufsize) {
						insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
						tid = _query->tripleNodes[i].tripleNodeID;
						bufsize = EntityIDList[tid]->getSize();
					}
				}
			}
		}

		std::vector<EntityIDBuffer*> bufferlist;
		bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		ID* p = EntityIDList[tid]->getBuffer();
		int IDCount = EntityIDList[tid]->getIDCount();

		for (i = 0; i != bufsize; ++i) {
			uriTable->getURIById(URI, p[i * IDCount + resultPos[0]]);
#ifdef PRINT_RESULT
			std::cout << URI << std::endl;
#else
			resultPtr->push_back(URI);
#endif
		}
	} else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		ID sortID;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		i = 0;
		int sortKey;
		size_t tnodesize = _query->tripleNodes.size();
		while (true) {
			if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1)) {
				++i;
				i = i % tnodesize;
				continue;
			}
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			tempBitV = projBitV & nodeBitV;
			if (tempBitV == nodeBitV) {
				if (countOneBits(resultBitV) == 0) {
					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
					sortKey = EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSortKey();
				} else {
					tempBitV = nodeBitV & resultBitV;
					if (countOneBits(tempBitV) == 1) {
						ID key = ID(log((double) tempBitV) / log(2.0));
						sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					} else {
						++i;
						i = i % tnodesize;
						continue;
					}
				}

				resultBitV |= nodeBitV;
				EntityIDList[_query->tripleNodes[i].tripleNodeID]->setSortKey(sortKey);
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);

				if (countOneBits(resultBitV) == projectionSize)
					break;
			}

			++i;
			i = i % tnodesize;
		}

		for (i = 0; i < bufferlist.size(); ++i) {
			bufferlist[i]->sort();
		}

		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		if (projectionSize == 2) {
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			ID* ids = buf->getBuffer();
			int IDCount = buf->getIDCount();
			for (i = 0; i < bufsize; i++) {
				for (int j = 0; j < IDCount; ++j) {
					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
#ifdef PRINT_RESULT
						cout << URI << " ";
#else
						resultPtr->push_back(URI);
#endif
					} else {
						cout << "not found" << endl;
					}
				}
#ifdef PRINT_RESULT
				cout << endl;
#endif
			}
		} else {
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.resize(bufferlist.size(), 0);
			int IDCount = buf->getIDCount();
			ID *ids = buf->getBuffer();
			for (i = 0; i != bufsize; ++i) {
				resultVec.resize(0);
				for (int j = 0; j < IDCount; ++j) {
					resultVec.push_back(ids[i * IDCount + j]);
				}

				getResult_join(resultVec[keyPos[0]], bufferlist, 1);
			}
		}
	}
	return OK;
}

Status TripleBitWorkerQuery::cyclicJoin() {
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node = NULL;
	EntityIDBuffer* buffer = NULL;
	TripleNode* triple = NULL;

	//initialize the patterns which are related to the first variable
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();
	getTripleNodeByID(triple, nodePatternIter->first);
	buffer = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
	EntityIDList[nodePatternIter->first] = buffer;

	if (this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT) {
#ifdef PRINT_RESULT
		cout << "empty result" << endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
		return OK;
	}

	++joinNodeIter;
	//iterate the pattern groups
	for (; joinNodeIter != _query->joinVariables.end(); ++joinNodeIter) {
		getVariableNodeByID(node, *joinNodeIter);
		if (node->appear_tpnodes.size() > 1) {
			if (this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	//iterate reverse
	bool isLeafNode;
	TripleBitQueryGraph::JoinVariableNodeID varID;
	size_t size = (int) _query->joinVariables.size();
	size_t i = 0;

	for (i = size - 1; i != ((size_t) (-1)); --i) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == false) {
			getVariableNodeByID(node, varID);
			if (this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}


	for (i = 0; i < size; ++i) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == false) {
			getVariableNodeByID(node, varID);
			if (this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
				cout << "The forth findEntitiesAndJoin" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	for (i = size - 1; i != ((size_t) (-1)); --i) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == false) {
			getVariableNodeByID(node, varID);
			if (this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout << "empty result" << endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	//TODO materialize
	std::vector<EntityIDBuffer*> bufferlist;
	std::vector<ID> resultVar;
	ID sortID;

	resultVar.resize(0);
	uint projBitV, nodeBitV, resultBitV, tempBitV;
	resultBitV = 0;
	generateProjectionBitVector(projBitV, _queryGraph->getProjection());

	int sortKey;
	size_t tnodesize = _query->tripleNodes.size();
	std::set<ID> tids;
	ID tid;
	bool complete = true;
	i = 0;
	vector<ID>::iterator iter;

	keyPos.clear();
	resultPos.clear();
	verifyPos.clear();

	while (true) {
		//if the pattern has no buffer, then skip it
		if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0) {
			++i;
			i = i % tnodesize;
			continue;
		}
		generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
		if (countOneBits(nodeBitV) == 1) {
			++i;
			i = i % tnodesize;
			continue;
		}

		tid = _query->tripleNodes[i].tripleNodeID;
		if (tids.count(tid) == 0) {
			if (countOneBits(resultBitV) == 0) {
				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				sortKey = EntityIDList[tid]->getSortKey();
			} else {
				tempBitV = nodeBitV & resultBitV;
				if (countOneBits(tempBitV) == 1) {
					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				} else if (countOneBits(tempBitV) == 2) {
					//verify buffers
					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				} else {
					complete = false;
					++i;
					i = i % tnodesize;
					continue;
				}
			}
			resultBitV = resultBitV | nodeBitV;
			EntityIDList[tid]->setSortKey(sortKey);
			bufferlist.push_back(EntityIDList[tid]);
			tids.insert(tid);
		}

		++i;
		if (i == tnodesize) {
			if (complete == true)
				break;
			else {
				complete = true;
				i = i % tnodesize;
			}
		}
	}

	for (i = 0; i < bufferlist.size(); ++i) {
		bufferlist[i]->sort();
	}

	generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
	//generate verify pos vector
	generateVerifyPos(resultVar, verifyPos);
	needselect = true;

#ifdef COLDCACHE
	Timestamp t1;
#endif

	EntityIDBuffer* buf = bufferlist[0];
	size_t bufsize = buf->getSize();

	int IDCount = buf->getIDCount();
	ID *ids = buf->getBuffer();
	sortKey = buf->getSortKey();
	for (i = 0; i != bufsize; ++i) {
		resultVec.resize(0);
		for (int j = 0; j < IDCount; ++j) {
			resultVec.push_back(ids[i * IDCount + j]);
		}

		getResult_join(resultVec[keyPos[0]], bufferlist, 1);
	}

#ifdef COLDCACHE
	Timestamp t2;
	cout << "getResult_Join time:" << (static_cast<double>(t2-t1)/1000.0) << endl;
#endif

	return OK;
}
*/

