/*
 * mine.cpp
 *
 *  Created on: 2017年4月25日
 *      Author: XuQingQing
 */

#include "TripleBitBuilder.h"
#include "OSFile.h"
#include "BitmapBuffer.h"
#include "PredicateTable.h"
#include <fstream>

using namespace std;

//mine
char* DATABASE_PATH;
int main(int argc, char* argv[]) {
	DATABASE_PATH = "/home/xuqingqing/code/xTripleBit/data/";

	PredicateTable* predicates = PredicateTable::load("/home/xuqingqing/code/xTripleBit/data/");
	vector<ID> ids;
	predicates->getAllPredicateIDs(ids);
	for(size_t i = 0; i < ids.size(); i++){
		cout << ids[i] << endl;
	}

	cout << "over" << endl;
	return 0;
}
