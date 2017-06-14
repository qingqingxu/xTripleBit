/*
 * TripleBitBuilder.cpp
 *
 *  Created on: Apr 6, 2010
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "MMapBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitBuilder.h"
#include "PredicateTable.h"
#include "TripleBit.h"
#include "URITable.h"
#include "Sorter.h"
#include "StatisticsBuffer.h"

#include <string.h>
#include <pthread.h>
#include <fstream>

//#define MYDEBUG

static int getCharPos(const char* data, char ch)
{
	const char * p = data;
	int i = 0;
	while(*p != '\0'){
		if( *p == ch)
			return i+1;
		p++;
		i++;
	}

	return -1;
}

TripleBitBuilder::TripleBitBuilder(string _dir) : dir(_dir) {
	// TODO Auto-generated constructor stub
	preTable = new PredicateTable(dir);
	uriTable = new URITable(dir);
	bitmap = new BitmapBuffer(dir);

	statBuffer[0] = new TwoConstantStatisticsBuffer(string(dir + "/subjectpredicate_statis"), StatisticsBuffer::SUBJECTPREDICATE_STATIS);	//subject-predicate statistics buffer;
	statBuffer[1] = new TwoConstantStatisticsBuffer(string(dir + "/objectpredicate_statis"), StatisticsBuffer::OBJECTPREDICATE_STATIS);	//object-predicate statistics buffer;

	staReifTable = new StatementReificationTable();
}

TripleBitBuilder::TripleBitBuilder() {
	preTable = NULL;
	uriTable = NULL;
	bitmap = NULL;
	staReifTable = NULL;
}

TripleBitBuilder::~TripleBitBuilder() {
	// TODO Auto-generated destructor stub
#ifdef TRIPLEBITBUILDER_DEBUG
	cout << "Bit map builder destroyed begin " << endl;
#endif
	//mysql = NULL;
	if( preTable != NULL ) 
		delete preTable;
	preTable = NULL;

	if( uriTable != NULL )
		delete uriTable;
	uriTable = NULL;
	//delete uriStaBuffer;
	if ( staReifTable != NULL )
		delete staReifTable;
	staReifTable = NULL;

	if (bitmap != NULL) {
		delete bitmap;
		bitmap = NULL;
	}

	for(int i = 0; i < 2; i++)
	{
		if(statBuffer[i] != NULL)
			delete statBuffer[i];
		statBuffer[i] = NULL;
	}
}

bool TripleBitBuilder::isStatementReification(const char* object) {
	int pos;

	const char* p;

	if ((pos = getCharPos(object, '#')) != -1) {
		p = object + pos;

		if (strcmp(p, "Statement") == 0 || strcmp(p, "subject") == 0 || strcmp(p, "predicate") == 0 || strcmp(p, "object") == 0) {
			return true;
		}
	}

	return false;
}

void TripleBitBuilder::NTriplesParse(const char* subject,  const char* predicate, const char* object, TempFile& facts) {
	ID subjectID, objectID, predicateID;

	if (isStatementReification(object) == false && isStatementReification(predicate) == false) {
		if (preTable->getIDByPredicate(predicate, predicateID) == PREDICATE_NOT_BE_FINDED) {
			preTable->insertTable(predicate, predicateID);
		}


		if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND)
			uriTable->insertTable(subject, subjectID);
		if (uriTable->getIdByURI(object, objectID) == URI_NOT_FOUND)
			uriTable->insertTable(object, objectID);

		facts.writeTriple(subjectID, predicateID, objectID, predicateObjTypes[predicateID]);
	} else {
//		statementFile << subject << " : " << predicate << " : " << object << endl;
	}

}

bool TripleBitBuilder::N3Parse(istream& in, const char* name, TempFile& rawFacts) {
	cerr << "Parsing " << name << "..." << endl;

	TurtleParser parser(in);
	try {
		string subject, predicate, object, source;
		while (true) {
			try {
				if (!parser.parse(subject, predicate, object))
					break;
			} catch (const TurtleParser::Exception& e) {
				while (in.get() != '\n');
				continue;
			}
			//Construct IDs
			//and write the triples
			if(subject.length() && predicate.length() && object.length())
				NTriplesParse((char*) subject.c_str(), (char*) predicate.c_str(), (char*) object.c_str(), rawFacts);

		}
	} catch (const TurtleParser::Exception&) {
		return false;
	}
	return true;
}

const uchar* TripleBitBuilder::skipIdIdId(const uchar* reader) {
	uint predicate;
	const uchar* temp = TempFile::skipId(TempFile::skipId(reader, DataType::STRING), DataType::STRING);
	return TempFile::skipId(temp, predicateObjTypes[predicate]);
}

int TripleBitBuilder::compare213(const uchar* left, const uchar* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l2, l1, l3, r2, r1, r3);
}

int TripleBitBuilder::compare231(const uchar* left, const uchar* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l2, l3, l1, r2, r3, r1);
}

int TripleBitBuilder::compare123(const uchar* left, const uchar* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l1, l2, l3, r1, r2, r3);
}

int TripleBitBuilder::compare321(const uchar* left, const uchar* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l3, l2, l1, r3, r2, r1);
}

void print(TempFile& infile, char* outfile){
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(infile.getFile().c_str()));
	const char* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

	// Produce tempfile
	ofstream out(outfile);
	while (reader < limit) {
		out << *(ID*)reader << "\t" << *(ID*)(reader + 4) << "\t" << *(ID*)(reader + 8) << endl;
		reader += 12;
	}
	mappedIn.close();
	out.close();
}

Status TripleBitBuilder::resolveTriples(TempFile& rawFacts, TempFile& facts) {
	cout<<"Sort by Subject"<<endl;

	varType subject, object, lastSubject, lastObject;
	ID predicateID, lastPredicate = 0;

	unsigned count1 = 0;
	TempFile sortedBySubject("./SortByS"), sortedByObject("./SortByO");
	Sorter::sort(rawFacts, sortedBySubject, skipIdIdId, compare213);
#ifdef MYDEBUG
	print(sortedBySubject, "sortedBySubject_temp");
#endif
	Sorter::sort(rawFacts, sortedBySubject, skipIdIdId, compare123);
	{
		//insert into chunk
		sortedBySubject.close();
		MemoryMappedFile mappedIn;
		assert(mappedIn.open(sortedBySubject.getFile().c_str()));
		const uchar* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

		loadTriple(reader, subject, predicateID, object);
		lastSubject = subject; lastPredicate = predicateID; lastObject = object;
		reader = skipIdIdId(reader);
		bitmap->insertTriple(predicateID, subject, object, OrderByType::ORDERBYS);
		count1 = 1;
		
		while (reader < limit) {
			loadTriple(reader, subject, predicateID, object);
			if(lastSubject == subject && lastPredicate == predicateID && lastObject == object) {
				reader = skipIdIdId(reader);
				continue;
			}

			if ( subject.var_uint != lastSubject.var_uint ) {
				statBuffer[0]->addStatis(lastSubject, lastPredicate, count1);
				lastPredicate = predicateID;
				lastSubject = subject;
				count1 = 1;
			} else if ( predicateID != lastPredicate ) {
				statBuffer[0]->addStatis(lastSubject, lastPredicate, count1);
				lastPredicate = predicateID;
				count1 = 1;
			}else {
				count1++;
				lastObject = object;
			}
			
			reader = reader + 12;
			bitmap->insertTriple(predicateID, subject, object, OrderByType::ORDERBYS);
		}
		mappedIn.close();
	}

	bitmap->flush();

	//sort
	cerr << "Sort by Object" << endl;
	Sorter::sort(rawFacts, sortedByObject, skipIdIdId, compare231);
#ifdef MYDEBUG
	print(sortedByObject, "sortedByObject_temp");
#endif
	Sorter::sort(rawFacts, sortedByObject, skipIdIdId, compare321);
	{
		//insert into chunk
		sortedByObject.close();
		MemoryMappedFile mappedIn;
		assert(mappedIn.open(sortedByObject.getFile().c_str()));
		const uchar* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

		loadTriple(reader, subject, predicateID, object);
		lastSubject = subject; lastPredicate = predicateID; lastObject = object;
		reader = skipIdIdId(reader);
		bitmap->insertTriple(predicateID, object, subject, OrderByType::ORDERBYO);
		count1 = 1;

		while (reader < limit) {
			loadTriple(reader, subject, predicateID, object);
			 if(lastSubject == subject && lastPredicate == predicateID && lastObject == object) {
				reader = skipIdIdId(reader);
				continue;
			}

			if ( object != lastObject ) {
				statBuffer[1]->addStatis(lastObject, lastPredicate, count1);
				lastPredicate = predicateID;
				lastObject = object;
				count1 = 1;
			} else if ( predicateID != lastPredicate ) {
				statBuffer[1]->addStatis(lastObject, lastPredicate, count1);
				lastPredicate = predicateID;
				count1 = 1;
			} else {
				lastSubject = subject;
				count1++;
			}
			reader = skipIdIdId(reader);
			// 1 indicate the triple is sorted by objects' id;
			bitmap->insertTriple(predicateID, object, subject, OrderByType::ORDERBYO);
		}
		mappedIn.close();
	}

	bitmap->flush();
	rawFacts.discard();
	sortedByObject.discard();
	sortedBySubject.discard();

	return OK;
}

Status TripleBitBuilder::startBuildN3(string fileName) {
	TempFile rawFacts("./test");

	ifstream in((char*) fileName.c_str());
	if (!in.is_open()) {
		cerr << "Unable to open " << fileName << endl;
		return ERROR;
	}
	if (!N3Parse(in, fileName.c_str(), rawFacts)) {
		in.close();
		return ERROR;
	}

	in.close();
	delete uriTable;
	uriTable = NULL;
	delete preTable;
	preTable = NULL;
	delete staReifTable;
	staReifTable = NULL;

	rawFacts.flush();
	cout<<"over"<<endl;

	//sort by s,o
	TempFile facts(fileName);
	resolveTriples(rawFacts, facts);
	facts.discard();
	return OK;
}

Status TripleBitBuilder::buildIndex() {
	// build hash index;
	MMapBuffer* bitmapIndex;
	cout<<"build hash index for subject"<<endl;
	for ( map<ID,ChunkManager*>::iterator iter = bitmap->predicate_managers[0].begin(); iter != bitmap->predicate_managers[0].end(); iter++ ) {
		if ( iter->second != NULL ) {
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
		}
	}

	cout<<"build hash index for object"<<endl;
	for ( map<ID, ChunkManager*>::iterator iter = bitmap->predicate_managers[1].begin(); iter != bitmap->predicate_managers[1].end(); iter++ ) {
		if ( iter->second != NULL ) {
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
		}
	}

	return OK;
}

Status TripleBitBuilder::endBuild() {
	bitmap->save();

	ofstream ofile(string(dir + "/statIndex").c_str());
	MMapBuffer* indexBuffer = NULL;
	((OneConstantStatisticsBuffer*)statBuffer[0])->save(indexBuffer);
	((OneConstantStatisticsBuffer*)statBuffer[1])->save(indexBuffer);
	((TwoConstantStatisticsBuffer*)statBuffer[2])->save(indexBuffer);
	((TwoConstantStatisticsBuffer*)statBuffer[3])->save(indexBuffer);

	delete indexBuffer;
	return OK;
}
