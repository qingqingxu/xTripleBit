/*
 * TripleBitWorkerTT.h
 *
 *  Created on: 2014-2-11
 *      Author: root
 */

#ifndef TRIPLEBITWORKERTT_H_
#define TRIPLEBITWORKERTT_H_

class BitmapBuffer;
class URITable;
class PredicateTable;
class TripleBitRepository;
class TripleBitQueryGraph;
class EntityIDBuffer;
class HashJoin;
class SPARQLLexer;
class SPARQLParser;
class QuerySemanticAnalysis;
class PlanGenerator;
class TasksQueueWP;
class TripleBitWorkerQuery;

#include "TripleBit.h"
#include "comm/TransQueueSWTT.h"

class TripleBitWorkerTT{
private:
	TripleBitRepository *tripleBitRepo;
	PredicateTable *preTable;
	URITable *uriTable;
	BitmapBuffer *bitmapBuffer;
	TransQueueSWTT *transQueSWTT;

	QuerySemanticAnalysis* semAnalysis;
	PlanGenerator* planGen;
	TripleBitQueryGraph* queryGraph;
	vector<string> resultSet;
	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;

	ID workerID;

	boost::mutex* uriMutex;

	TripleBitWorkerQuery* workerQuery;
	Transaction *trans;

public:
	TripleBitWorkerTT(TripleBitRepository* repo, ID workID);
	Status Execute(string &queryString);
	Status Execute(Transaction *transaction);
	~TripleBitWorkerTT(){}
	void Work();
	void WorkForTT(Transaction *transaction);
	void sleepForDelay(unsigned time);
};

#endif /* TRIPLEBITWORKERTT_H_ */
