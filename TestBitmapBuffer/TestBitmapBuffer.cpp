/*
 * mine.cpp
 *
 *  Created on: 2017年4月25日
 *      Author: XuQingQing
 */

#include "TripleBit.h"
#include "TripleBitBuilder.h"
#include "OSFile.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include <fstream>

using namespace std;

//mine
char* DATABASE_PATH;
int main(int argc, char* argv[]) {
	DATABASE_PATH = "/home/xuqingqing/code/xTripleBit/data/";

	string bitmapBuffer = "/home/xuqingqing/code/xTripleBit/data/BitmapBuffer";

	MMapBuffer* buffer = MMapBuffer::create(bitmapBuffer.c_str(), 0);
	uchar* bufferReader = buffer->get_address();

	string predicate = bitmapBuffer.append("_predicate");
	MMapBuffer* predicateBuffer = MMapBuffer::create(predicate.c_str(), 0);
	uchar* predicateReader = predicateBuffer->get_address();
	uchar* predicateLimit = predicateReader + predicateBuffer->getSize();
	ID predicateID, subjectID;
	double object;
	char objType;
	bool soType;
	size_t offset = 0;
	bool isFirstPage = true;
	ofstream sp("bitmapbuffer_sp", ios::app);
	ofstream op("bitmapbuffer_op", ios::app);
	ChunkManagerMeta* meta;

	while (predicateReader < predicateLimit) {
		isFirstPage = true;
		predicate = *(ID*) predicateReader;
		predicateReader += sizeof(ID);
		soType = *(bool*) predicateReader;
		predicateReader += sizeof(bool);
		offset = *(size_t*) predicateReader;
		predicateReader += sizeof(size_t) * 2;
		bufferReader += offset;

		meta = (ChunkManagerMeta*) bufferReader;
		bufferReader += sizeof(ChunkManagerMeta);

		if (meta->soType == ORDERBYS) {
			uchar* endChunkManager = bufferReader - sizeof(ChunkManagerMeta)
					+ meta->length;
			while (bufferReader < endChunkManager) {
				MetaData* metaData = (MetaData*) bufferReader;
				uchar* endPtr = bufferReader + metaData->usedSpace;
				bufferReader += sizeof(MetaData);
				while (bufferReader < endPtr) {
					bufferReader = Chunk::readID(bufferReader, subjectID);
					bufferReader = Chunk::read(bufferReader, objType, CHAR);
					bufferReader = Chunk::read(bufferReader, object, objType);
					sp << subjectID << "\t" << meta->pid << "\t" << object
							<< endl;
				}
			}

		} else if (meta->soType == ORDERBYO) {
			uchar* endChunkManager = bufferReader - sizeof(ChunkManagerMeta)
					+ meta->length;
			while (bufferReader < endChunkManager) {
				MetaData* metaData = (MetaData*) bufferReader;
				uchar* endPtr = bufferReader + metaData->usedSpace;
				bufferReader += sizeof(MetaData);
				while (bufferReader < endPtr) {
					bufferReader = Chunk::read(bufferReader, objType, CHAR);
					bufferReader = Chunk::read(bufferReader, object, objType);
					bufferReader = Chunk::readID(bufferReader, subjectID);
					op << subjectID << "\t" << meta->pid << "\t" << object
							<< endl;
				}
			}
		}
	}

	sp.close();
	op.close();

	cout << "over" << endl;
	return 0;
}
