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
#include "StatisticsBuffer.h"
#include "TripleBit.h"
#include "URITable.h"
#include "Sorter.h"

#include <string.h>
#include <pthread.h>
#include <fstream>

#define MYDEBUG

static int getCharPos(const char* data, char ch) {
	const char * p = data;
	int i = 0;
	while (*p != '\0') {
		if (*p == ch)
			return i + 1;
		p++;
		i++;
	}

	return -1;
}

TripleBitBuilder::TripleBitBuilder(string _dir) :
		dir(_dir) {
	// TODO Auto-generated constructor stub
	preTable = new PredicateTable(dir);
	uriTable = new URITable(dir);
	bitmap = new BitmapBuffer(dir);

	spStatisBuffer = new StatisticsBuffer(
			string(dir + "/subjectpredicate_statis"), SUBJECTPREDICATE_STATIS); //subject-predicate statistics buffer;
	opStatisBuffer = new StatisticsBuffer(
			string(dir + "/objectpredicate_statis"), OBJECTPREDICATE_STATIS); //object-predicate statistics buffer;

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
	if (preTable != NULL)
		delete preTable;
	preTable = NULL;

	if (uriTable != NULL)
		delete uriTable;
	uriTable = NULL;
	//delete uriStaBuffer;
	if (staReifTable != NULL)
		delete staReifTable;
	staReifTable = NULL;

	if (bitmap != NULL) {
		delete bitmap;
		bitmap = NULL;
	}

	if (spStatisBuffer != NULL) {
		delete spStatisBuffer;
	}
	spStatisBuffer = NULL;

	if (opStatisBuffer != NULL) {
		delete opStatisBuffer;
	}
	opStatisBuffer = NULL;

}

bool TripleBitBuilder::isStatementReification(const char* object) {
	int pos;

	const char* p;

	if ((pos = getCharPos(object, '#')) != -1) {
		p = object + pos;

		if (strcmp(p, "Statement") == 0 || strcmp(p, "subject") == 0
				|| strcmp(p, "predicate") == 0 || strcmp(p, "object") == 0) {
			return true;
		}
	}

	return false;
}

/*string strim(string &s) {
	if (s.empty()) {
		return s;
	}
	s.erase(0, s.find_first_not_of(" "));
	s.erase(s.find_last_not_of(" ") + 1);
	return s;
}*/

bool lexDate(string &str, double& date) {
	if (str.empty()) {
		return false;
	}
	TurtleParser::strim(str);
	if (str.empty() || str.length() != 19) {
		return false;
	}
	if (str[0] >= '0' && str[0] <= '9' && str[1] >= '0' && str[1] <= '9'
			&& str[2] >= '0' && str[2] <= '9' && str[3] >= '0' && str[3] <= '9'
			&& str[4] == '-' && str[5] >= '0' && str[5] <= '1' && str[6] >= '0'
			&& str[6] <= '9' && str[7] == '-' && str[8] >= '0' && str[8] <= '3'
			&& str[9] >= '0' && str[9] <= '9' && str[10] == ' '
			&& str[11] >= '0' && str[11] <= '2' && str[12] >= '0'
			&& str[12] <= '9' && str[13] == ':' && str[14] >= '0'
			&& str[14] <= '5' && str[15] >= '0' && str[15] <= '9'
			&& str[16] == ':' && str[17] >= '0' && str[17] <= '5'
			&& str[18] >= '0' && str[18] <= '9') {
		date = (str[0] - '0');
		date = date * 10 + (str[1] - '0');
		date = date * 10 + (str[2] - '0');
		date = date * 10 + (str[3] - '0');
		date = date * 10 + (str[5] - '0');
		date = date * 10 + (str[6] - '0');
		date = date * 10 + (str[8] - '0');
		date = date * 10 + (str[9] - '0');
		date = date * 10 + (str[11] - '0');
		date = date * 10 + (str[12] - '0');
		date = date * 10 + (str[14] - '0');
		date = date * 10 + (str[15] - '0');
		date = date * 10 + (str[17] - '0');
		date = date * 10 + (str[18] - '0');
		return true;
	}
	return false;
}

void TripleBitBuilder::NTriplesParse(const char* subject, const char* predicate,
		string& object, char& objType, TempFile& facts) {
	ID subjectID, predicateID, objectID;
	double tempObject;

	if (isStatementReification(subject) == false
			&& isStatementReification(predicate) == false) { //?object->subject
		if (preTable->getIDByPredicate(predicate, predicateID)
				== PREDICATE_NOT_BE_FINDED) {
			preTable->insertTable(predicate, predicateID);
		}

		if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND) {
			uriTable->insertTable(subject, subjectID);
		}

		switch (objType) {
		case BOOL:
		case CHAR:
			tempObject = (double) object[0];
			break;
		case INT: {
			longlong ll = atoll(object.c_str());
			if (ll >= INT_MIN && ll <= INT_MAX) {
				objType = INT;
			} else if (ll >= 0 && ll <= UINT_MAX) {
				objType = UNSIGNED_INT;
			} else if (ll >= LLONG_MIN && ll <= LLONG_MAX) {
				objType = LONGLONG;
			}
			tempObject = (double) ll;
			break;
		}
		case DOUBLE:
			tempObject = atof(object.c_str());
			if (tempObject == HUGE_VAL) {
				MessageEngine::showMessage("data convert to double error",
						MessageEngine::ERROR);
				cout << "object: " << object << endl;
				return;
			} else if (tempObject >= FLT_MIN && tempObject <= FLT_MAX) {
				objType = FLOAT;
			}
			break;

		case STRING:
			if (lexDate(object, tempObject)) {
				objType = DATE;
				break;
			}

			if (uriTable->getIdByURI(object.c_str(), objectID)
					== URI_NOT_FOUND) {
				uriTable->insertTable(object.c_str(), objectID);
				tempObject = objectID;
				break;
			}
			tempObject = objectID;
			break;
		default:
			break;
		}

/*
#ifdef MYDEBUG
		ofstream out("n3ID", ios::app);
		out << subject << "\t" << predicate << "\t" << object << endl;
		out << subjectID << "\t" << predicateID << "\t" << tempObject << endl;
		out.close();
#endif
*/
		facts.writeTriple(subjectID, predicateID, tempObject, objType);
	}

}

bool TripleBitBuilder::N3Parse(istream& in, const char* name,
		TempFile& rawFacts) {
	cerr << "Parsing " << name << "..." << endl;

	TurtleParser parser(in);
	try {
		string subject, predicate, object;
		char objType = NONE;
		while (true) {
			try {
				if (!parser.parse(subject, predicate, object, objType))
					break;
			} catch (const TurtleParser::Exception& e) {
				while (in.get() != '\n')
					;
				continue;
			}
			//Construct IDs
			//and write the triples
			if (subject.length() && predicate.length() && objType != NONE) {
				NTriplesParse(subject.c_str(), predicate.c_str(), object,
						objType, rawFacts);
			} else {
				MessageEngine::showMessage("N3Parse error.",
						MessageEngine::ERROR);
			}

		}
	} catch (const TurtleParser::Exception&) {
		return false;
	}
	return true;
}

const uchar* TripleBitBuilder::skipIdIdId(const uchar* reader) {
	return TempFile::skipObject(TempFile::skipId(TempFile::skipId(reader)));
}

int TripleBitBuilder::compare213(const uchar* left, const uchar* right) {
	ID l1, l2, r1, r2;
	double l3, r3;
	char l4, r4;
	loadTriple(left, l1, l2, l3, l4);
	loadTriple(right, r1, r2, r3, r4);

	return cmpTriples(l2, l1, l3, l4, r2, r1, r3, r4);
}

int TripleBitBuilder::compare231(const uchar* left, const uchar* right) {
	ID l1, l2, r1, r2;
	double l3, r3;
	char l4, r4;
	loadTriple(left, l1, l2, l3, l4);
	loadTriple(right, r1, r2, r3, r4);

	return cmpTriples(l2, l3, l1, l4, r2, r3, r1, r4);
}

int TripleBitBuilder::compare123(const uchar* left, const uchar* right) {
	ID l1, l2, r1, r2;
	double l3, r3;
	char l4, r4;
	loadTriple(left, l1, l2, l3, l4);
	loadTriple(right, r1, r2, r3, r4);

	return cmpTriples(l1, l2, l3, l4, r1, r2, r3, r4);
}

int TripleBitBuilder::compare321(const uchar* left, const uchar* right) {
	ID l1, l2, r1, r2;
	double l3, r3;
	char l4, r4;
	loadTriple(left, l1, l2, l3, l4);
	loadTriple(right, r1, r2, r3, r4);

	return cmpTriples(l3, l2, l1, l4, r3, r2, r1, r4);
}

void print(TempFile& infile, char* outfile) {
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(infile.getFile().c_str()));
	const uchar* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

	// Produce tempfile
	ofstream out(outfile);
	while (reader < limit) {
		out << *(ID*) reader << "\t" << *(ID*) (reader + 4) << "\t"
				<< *(double*) (reader + 8) << *(char*)(reader + 16)/*getDataType(*(char*)(reader + 16))*/ << endl;
		reader += 17;
	}
	mappedIn.close();
	out.close();
}

Status TripleBitBuilder::resolveTriples(TempFile& rawFacts, TempFile& facts) {
	cout << "Sort by Subject" << endl;

	ID subjectID, lastSubjectID = 0, predicateID, lastPredicateID = 0;
	double object, lastObject;
	char objType, lastObjType;

	size_t count1 = 0;
	TempFile sortedBySubject("./SortByS"), sortedByObject("./SortByO");
#ifdef MYDEBUG
	print(rawFacts, "sortedBySubject_temp_unsort");
#endif
	Sorter::sort(rawFacts, sortedBySubject, skipIdIdId, compare123);
#ifdef MYDEBUG
	print(sortedBySubject, "sortedBySubject_temp");
#endif
	{
		//insert into chunk
		sortedBySubject.close();
		MemoryMappedFile mappedIn;
		assert(mappedIn.open(sortedBySubject.getFile().c_str()));
		const uchar* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

		loadTriple(reader, subjectID, predicateID, object, objType);
		lastSubjectID = subjectID;
		lastPredicateID = predicateID;
		lastObject = object;
		reader = skipIdIdId(reader);
		bitmap->insertTriple(predicateID, subjectID, object, ORDERBYS, objType);
		count1 = 1;

		while (reader < limit) {
			loadTriple(reader, subjectID, predicateID, object, objType);
			if (lastSubjectID == subjectID && lastPredicateID == predicateID
					&& lastObject == object) {
				reader = skipIdIdId(reader);
				continue;
			}

			if (subjectID != lastSubjectID) {
				spStatisBuffer->addStatis(lastSubjectID, lastPredicateID,
						count1);
				lastPredicateID = predicateID;
				lastSubjectID = subjectID;
				lastObject = object;
				count1 = 1;
			} else if (predicateID != lastPredicateID) {
				spStatisBuffer->addStatis(lastSubjectID, lastPredicateID,
						count1);
				lastPredicateID = predicateID;
				count1 = 1;
			} else {
				count1++;
				lastObject = object;
			}

			reader = skipIdIdId(reader);
			bitmap->insertTriple(predicateID, subjectID, object, ORDERBYS,
					objType);
		}
		mappedIn.close();
	}

	bitmap->flush();

	//sort
	cerr << "Sort by Object" << endl;
	Sorter::sort(rawFacts, sortedByObject, skipIdIdId, compare321);
#ifdef MYDEBUG
	print(sortedByObject, "sortedByObject_temp");
#endif
	{
		//insert into chunk
		sortedByObject.close();
		MemoryMappedFile mappedIn;
		assert(mappedIn.open(sortedByObject.getFile().c_str()));
		const uchar* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

		loadTriple(reader, subjectID, predicateID, object, objType);
		lastSubjectID = subjectID;
		lastPredicateID = predicateID;
		lastObject = object;
		lastObjType = objType;
		reader = skipIdIdId(reader);
		bitmap->insertTriple(predicateID, subjectID, object, ORDERBYO, objType);
		count1 = 1;

		while (reader < limit) {
			loadTriple(reader, subjectID, predicateID, object, objType);
			if (lastSubjectID == subjectID && lastPredicateID == predicateID
					&& lastObject == object) {
				reader = skipIdIdId(reader);
				continue;
			}

			if (object != lastObject) {
				opStatisBuffer->addStatis(lastObject, lastPredicateID, count1,
						lastObjType);
				lastPredicateID = predicateID;
				lastObject = object;
				lastObjType = objType;
				count1 = 1;
			} else if (predicateID != lastPredicateID) {
				opStatisBuffer->addStatis(lastObject, lastPredicateID, count1,
						lastObjType);
				lastPredicateID = predicateID;
				count1 = 1;
			} else {
				lastSubjectID = subjectID;
				count1++;
			}
			reader = skipIdIdId(reader);
			// 1 indicate the triple is sorted by objects' id;
			bitmap->insertTriple(predicateID, subjectID, object, ORDERBYO,
					objType);
		}
		mappedIn.close();
	}

	bitmap->flush();
	rawFacts.discard();
	sortedByObject.discard();
	sortedBySubject.discard();
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
	cout << "over" << endl;

	//sort by s,o
	TempFile facts(fileName);
	resolveTriples(rawFacts, facts);
	facts.discard();
	return OK;
}

Status TripleBitBuilder::buildIndex() {
	// build hash index;
	MMapBuffer* bitmapIndex;
	cout << "build hash index for subject" << endl;
	for (map<ID, ChunkManager*>::iterator iter =
			bitmap->predicate_managers[0].begin();
			iter != bitmap->predicate_managers[0].end(); iter++) {
		if (iter->second != NULL) {
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex()->save(bitmapIndex);
		}
	}

	cout << "build hash index for object" << endl;
	for (map<ID, ChunkManager*>::iterator iter =
			bitmap->predicate_managers[1].begin();
			iter != bitmap->predicate_managers[1].end(); iter++) {
		if (iter->second != NULL) {
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex()->save(bitmapIndex);
		}
	}

	return OK;
}

Status TripleBitBuilder::endBuild() {
	bitmap->save();

	ofstream ofile(string(dir + "/statIndex").c_str());
	MMapBuffer* indexBuffer = NULL;
	spStatisBuffer->save(indexBuffer);
	opStatisBuffer->save(indexBuffer);

	delete indexBuffer;
	return OK;
}
