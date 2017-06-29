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
#include "MemoryBuffer.h"
#include <fstream>

using namespace std;

//mine
char* DATABASE_PATH;
int main(int argc, char* argv[]) {
	DATABASE_PATH = "/home/xuqingqing/code/xTripleBit/data/";

	string bitmapBuffer = "/home/xuqingqing/code/xTripleBit/data/BitmapBuffer";

	MMapBuffer* buffer = MMapBuffer::create(bitmapBuffer.c_str(), 0);
	const uchar* bufferReader = (const uchar*) buffer->get_address();
	const uchar* startBuffer = bufferReader;

	string predicate = bitmapBuffer.append("_predicate");
	MMapBuffer* predicateBuffer = MMapBuffer::create(predicate.c_str(), 0);
	const uchar* predicateReader =
			(const uchar*) predicateBuffer->get_address();
	const uchar* predicateLimit = predicateReader + predicateBuffer->getSize();
	ID subjectID;
	double object;
	char objType;
	bool soType;
	size_t offset = 0;
	bool isFirstPage = true;
	/*ofstream sp("bitmapbuffer_sp_del", ios::app);
	ofstream op("bitmapbuffer_op_del", ios::app);*/
	ChunkManagerMeta* meta;

	while (predicateReader < predicateLimit) {
		isFirstPage = true;
		predicate = *(ID*) predicateReader;
		predicateReader += sizeof(ID);
		soType = *(bool*) predicateReader;
		predicateReader += sizeof(bool);
		offset = *(size_t*) predicateReader;
		predicateReader += sizeof(size_t) * 2;
		bufferReader = startBuffer + offset;

		meta = (ChunkManagerMeta*) bufferReader;
		cout << meta->pid << "\t" << meta->soType << "\t" << meta->pid << "\t" << meta->tripleCount << endl;
		/*bufferReader += sizeof(ChunkManagerMeta);

		if (meta->soType == ORDERBYS) {
			const uchar* endChunkManager = bufferReader
					- sizeof(ChunkManagerMeta) + meta->length;
			bool isFirstPage = true;
			while (bufferReader < endChunkManager) {
				MetaData* metaData = (MetaData*) bufferReader;
				const uchar* endPtr = bufferReader + metaData->usedSpace;
				bufferReader += sizeof(MetaData);
				while (bufferReader < endPtr) {
					bufferReader = Chunk::readID(bufferReader, subjectID);
					bufferReader = Chunk::read(bufferReader, objType, CHAR);
					bufferReader = Chunk::read(bufferReader, object, objType);
					sp << subjectID << "\t" << meta->pid << "\t" << object
							<< endl;
				}
				if (isFirstPage) {
					bufferReader = endPtr - metaData->usedSpace
							- sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
					isFirstPage = false;
				} else {
					bufferReader = endPtr - metaData->usedSpace
							+ MemoryBuffer::pagesize;
				}
			}

		} else if (meta->soType == ORDERBYO) {
			const uchar* endChunkManager = bufferReader
					- sizeof(ChunkManagerMeta) + meta->length;
			bool isFirstPage = true;
			while (bufferReader < endChunkManager) {
				MetaData* metaData = (MetaData*) bufferReader;
				const uchar* endPtr = bufferReader + metaData->usedSpace;
				bufferReader += sizeof(MetaData);
				while (bufferReader < endPtr) {
					bufferReader = Chunk::read(bufferReader, objType, CHAR);
					bufferReader = Chunk::read(bufferReader, object, objType);
					bufferReader = Chunk::readID(bufferReader, subjectID);
					op << subjectID << "\t" << meta->pid << "\t" << object
							<< endl;
				}
				if (isFirstPage) {
					bufferReader = endPtr - metaData->usedSpace
							- sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
					isFirstPage = false;
				} else {
					bufferReader = endPtr - metaData->usedSpace
							+ MemoryBuffer::pagesize;
				}
			}
		}*/
	}

	/*sp.close();
	op.close();*/

	cout << "over" << endl;
	return 0;
}