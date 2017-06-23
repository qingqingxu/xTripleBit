/*
 * mine.cpp
 *
 *  Created on: 2017年4月25日
 *      Author: XuQingQing
 */

#include "TripleBitBuilder.h"
#include "OSFile.h"
#include "BitmapBuffer.h"
#include <fstream>

using namespace std;

//mine

int main(int argc, char* argv[]) {


	string str = "/home/xuqingqing/code/xTripleBit/data/subjectpredicate_statis";
	cout << str << "................" << endl;
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(str.c_str()));
	const uchar* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
	ID subjectID, predicateID;
	size_t count;
	cout << "spc stat" << endl;
	ofstream out("spct", ios::app);
	while (reader < limit) {
		memcpy(&subjectID, reader, sizeof(ID));
		reader += sizeof(ID);
		memcpy(&predicateID, reader, sizeof(ID));
		reader += sizeof(ID);
		memcpy(&count, reader, sizeof(size_t));
		reader += sizeof(size_t);
		out << subjectID << "\t" << predicateID << "\t" << count << endl;
	}
	out.close();
	mappedIn.close();


	str = "/home/xuqingqing/code/xTripleBit/data/objectpredicate_statis";
		cout << str << "................" << endl;
		assert(mappedIn.open(str.c_str()));
		reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
		double object;
		char objType;
		size_t count;
		cout << "opc stat" << endl;
		ofstream out("opct", ios::app);
		while (reader < limit) {
			memcpy(&objType, reader, sizeof(char));
			reader += sizeof(char);
			reader = Chunk::read(reader, object, objType);
			reader += Chunk::getLen(objType);
			memcpy(&predicateID, reader, sizeof(ID));
			reader += sizeof(ID);
			memcpy(&count, reader, sizeof(size_t));
			reader += sizeof(size_t);
			out << object << "\t" << predicateID << "\t" << count << endl;
		}
		out.close();
		mappedIn.close();

	cout << "over" << endl;
	return 0;
}
