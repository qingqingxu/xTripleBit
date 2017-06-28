/*
 * TripleBitWorkerTT.cpp
 *
 *  Created on: 2014-2-11
 *      Author: root
 */

#include "TripleBitWorkerTT.h"
#include "SPARQLLexer.h"
#include "SPARQLParser.h"
#include "QuerySemanticAnalysis.h"
#include "PlanGenerator.h"
#include "TripleBitQueryGraph.h"
#include "TripleBitRepository.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"
#include "util/Timestamp.h"
#include "comm/TransQueueSWTT.h"
#include "TripleBitWorkerQuery.h"

TripleBitWorkerTT::TripleBitWorkerTT(TripleBitRepository *repo, ID workID): tripleBitRepo(repo), workerID(workID){
		preTable = repo->getPredicateTable();
		uriTable = repo->getURITable();
		bitmapBuffer = repo->getBitmapBuffer();
		transQueSWTT = repo->getTransQueueSWTT();
		uriMutex = repo->getUriMutex();

		queryGraph = new TripleBitQueryGraph();
		planGen = new PlanGenerator(*repo);
		semAnalysis = new QuerySemanticAnalysis(*repo);
		workerQuery = new TripleBitWorkerQuery(repo, workerID);
}

void TripleBitWorkerTT::sleepForDelay(unsigned time){
	if(!time){
		sched_yield();
	}
	else{
		struct timespec a, b;
		a.tv_sec = time/1000; a.tv_nsec=(time%1000)*1000000;
		nanosleep(&a, &b);
	}
}

void TripleBitWorkerTT::Work(){
	while(1){
		trans = transQueSWTT->DeQueue();
		string queryString = trans->queryString;

		if(queryString == "exit"){
			delete trans;
			tripleBitRepo->workerComplete();
			break;
		}

		sleepForDelay(trans->delay);

		Execute(queryString);

		trans->responseTime = getTicks() - transQueSWTT->getTimeBase() - trans->arrival;
		trans = NULL;
	}
}

void TripleBitWorkerTT::WorkForTT(Transaction *transaction){
	string queryString = transaction->queryString;

	if(queryString == "exit"){
		tripleBitRepo->workerComplete();
		return ;
	}

	Execute(queryString);
}

Status TripleBitWorkerTT::Execute(string &queryString){
#ifdef TRANSACTION_TIME
	Timestamp start;
#endif

	SPARQLLexer *lexer = new SPARQLLexer(queryString);
	SPARQLParser *parser = new SPARQLParser(*lexer);
	try{
		parser->parse();
	}catch(const SPARQLParser::ParserException &e){
		cerr << "Parser error: " << e.message << endl;
		return ERR;
	}

	if(parser->getOperationType() == SPARQLParser::QUERY){
		queryGraph->Clear();
		uriMutex->lock();
		if(!this->semAnalysis->transform(*parser, *queryGraph)){
			return NOT_FOUND;
		}
		uriMutex->unlock();

		if(queryGraph->knownEmpty() == true){
			cerr << "Empty result" << endl;
			return OK;
		}

		if(queryGraph->isPredicateConst() == false){
			resultSet.push_back("-1");
			resultSet.push_back("predicate should be constant");
			return ERR;
		}

		planGen->generatePlan(*queryGraph);

		timeval transTime;
		gettimeofday(&transTime, 0);

		workerQuery->query(queryGraph, resultSet, transTime);

#ifdef TRANSACTION_TIME
		Timestamp stop;
		cerr << " time slapsed: " << static_cast<double>(stop-start)/1000.0 << " s" << endl;
#endif

		workerQuery->releaseBuffer();
	}
	else{
		queryGraph->Clear();
		uriMutex->lock();
		if(!this->semAnalysis->transform(*parser, *queryGraph)){
			return ERR;
		}
		uriMutex->unlock();

		timeval transTime;
		gettimeofday(&transTime, 0);

		workerQuery->query(queryGraph, resultSet, transTime);

#ifdef TRANSACTION_TIME
		Timestamp stop;
		cerr << " time slapsed: " << static_cast<double>(stop-start)/1000.0 << " s" << endl;
#endif

		workerQuery->releaseBuffer();
	}
	delete lexer;
	delete parser;
	return OK;
}

Status TripleBitWorkerTT::Execute(Transaction *transaction){
	timeval transTime;
	gettimeofday(&transTime, 0);

	workerQuery->excuteInsertData(transaction->triples, transTime);
	return OK;
}


