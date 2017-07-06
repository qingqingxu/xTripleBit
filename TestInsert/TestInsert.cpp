/*
 * TripleBitQuery.cpp
 *
 *  Created on: Apr 12, 2011
 *      Author: root
 */

#include "../TripleBit/TripleBit.h"
#include "../TripleBit/TripleBitRepository.h"
#include "../TripleBit/OSFile.h"
#include "../TripleBit/MMapBuffer.h"

char* DATABASE_PATH;
char* QUERY_PATH;
int main(int argc, char* argv[]) {
	if (argc < 3) {
		fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]);
		return -1;
	}

	DATABASE_PATH = argv[1];
	QUERY_PATH = argv[2];

	TripleBitRepository* repo = TripleBitRepository::create(argv[1]);
	if (repo == NULL) {
		return -1;
	}


	repo->cmd_line_insert(argv[2]);

	return 0;
}
