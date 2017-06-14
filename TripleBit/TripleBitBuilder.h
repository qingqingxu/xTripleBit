/*
 * TripleBitBuilder.h
 *
 *  Created on: Apr 6, 2010
 *      Author: root
 *
 *  build tripleBit table,predicate table, URI table
 */

#ifndef TRIPLEBITBUILDER_H_
#define TRIPLEBITBUILDER_H_

#define TRIPLEBITBUILDER_DEBUG 1

class PredicateTable;
class URITable;
class URIStatisticsBuffer;
class StatementReificationTable;
class FindColumns;
class BitmapBuffer;
class Sorter;
class TempFile;
class StatisticsBuffer;

#include "TripleBit.h"
#include "StatisticsBuffer.h"

#include <fstream>
#include <pthread.h>
#include <cassert>
#include <cstring>

#include "TurtleParser.hpp"
#include "ThreadPool.h"
#include "TempFile.h"

using namespace std;

class TripleBitBuilder {
private:
	//MySQL* mysql;
	BitmapBuffer* bitmap;
	PredicateTable* preTable;
	URITable* uriTable;
	vector<string> predicates;
	string dir;
	/// statistics buffer;
	StatisticsBuffer* statBuffer[2];
	StatementReificationTable* staReifTable;
	FindColumns* columnFinder;

public:
	TripleBitBuilder();
	TripleBitBuilder(const string dir);
	static const uchar* skipIdIdId(const uchar* reader);
	static int compare213(const uchar* left, const uchar* right);
	static int compare231(const uchar* left, const uchar* right);
	static int compare123(const uchar* left, const uchar* right);
	static int compare321(const uchar* left, const uchar* right);

	static inline void loadTriple(const uchar* data, varType& v1, ID& v2, varType& v3) {
		const uchar* temp = TempFile::readID(TempFile::read(data, v1), v2);
		TempFile::read(temp, v3, predicateObjTypes[v2]);
	}

	template<typename T>
	static inline int cmpValue(T l ,T r) {
		return (l < r) ? -1 : ((l > r) ? 1 : 0);
	}

	template<typename T>
	static inline int cmpTriples(T l1, T l2, T l3, T r1, T r2, T r3){
		int c = cmpValue(l1, r1);
		if(c)
			return c;
		c = cmpValue(l2, r2);
		if(c)
			return c;
		return cmpValue(l3, r3);
	}
	StatisticsBuffer* getStatBuffer(StatisticsBuffer::StatisticsType type) {
		return statBuffer[static_cast<int>(type)];
	}

	Status resolveTriples(TempFile& rawFacts, TempFile& facts);
	Status startBuildN3(string fileName);
	bool N3Parse(istream& in, const char* name, TempFile&);
	Status importFromMySQL(string db, string server, string username, string password);
	void NTriplesParse(const char* subject, const char* predicate, const char* object, TempFile&);
	Status buildIndex();
	Status endBuild();
	
	static bool isStatementReification(const char* object);
	virtual ~TripleBitBuilder();
};
#endif /* TRIPLEBITBUILDER_H_ */
