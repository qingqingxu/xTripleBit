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
	StatisticsBuffer *spStatisBuffer, *opStatisBuffer;
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

	static inline void loadTriple(const uchar* data, ID& v1, ID& v2, double& v3, char& objType) {
		const uchar* temp = TempFile::readID(TempFile::readID(data, v1), v2);
		TempFile::read(temp, v3, objType);
	}

	template<typename T>
	static inline int cmpValue(T l ,T r) {
		return (l < r) ? -1 : ((l > r) ? 1 : 0);
	}

	template<typename T1, typename T2, typename T3>
	static inline int cmpTriples(T1 l1, T2 l2, T3 l3, T1 r1, T2 r2, T3 r3){
		int c = cmpValue(l1, r1);
		if(c)
			return c;
		c = cmpValue(l2, r2);
		if(c)
			return c;
		return cmpValue(l3, r3);
	}

	Status resolveTriples(TempFile& rawFacts, TempFile& facts);
	Status startBuildN3(string fileName);
	bool N3Parse(istream& in, const char* name, TempFile& rawFacts);
	Status importFromMySQL(string db, string server, string username, string password);
	void NTriplesParse(const char* subject, const char* predicate, string& object, char& objType, TempFile&);
	Status buildIndex();
	Status endBuild();
	
	static bool isStatementReification(const char* object);
	virtual ~TripleBitBuilder();
};
#endif /* TRIPLEBITBUILDER_H_ */
