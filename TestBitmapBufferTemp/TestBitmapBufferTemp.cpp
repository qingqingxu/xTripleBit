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
#include "TempMMapBuffer.h"
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

	string tempMMap(bitmapBuffer);
	tempMMap.append("_temp");
	TempMMapBuffer::create(tempMMap.c_str(), buffer->get_length());
	ID subjectID;
	double object;
	char objType;
	bool soType;
	size_t offset = 0;
	bool isFirstPage = true;
	ofstream sp("bitmapbuffer_sp_chunk_insert", ios::app);
	ofstream op("bitmapbuffer_op_chunk_insert", ios::app);
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
		bufferReader += sizeof(ChunkManagerMeta);

		if (meta->soType == ORDERBYS) {
			const uchar* endChunkManager = bufferReader
					- sizeof(ChunkManagerMeta) + meta->length;
			bool isFirstPage = true;
			int chunkId = 0;
			while (bufferReader < endChunkManager) {
				MetaData* metaData = (MetaData*) bufferReader;
				//sp << "ORDERBYS: " <<  meta->pid << "\t" << "chunk:" << chunkId << "\t" << metaData->usedSpace << "\t" << metaData->min << "\t" << metaData->max << endl;
				const uchar* endPtr = bufferReader + metaData->usedSpace;
				bufferReader += sizeof(MetaData);
				while (bufferReader < endPtr) {
					bufferReader = Chunk::readID(bufferReader, subjectID);
					bufferReader = Chunk::read(bufferReader, objType, CHAR);
					bufferReader = Chunk::read(bufferReader, object, objType);
					sp << "chunk," << chunkId << "," << subjectID << ","
							<< meta->pid << "," << object << endl;
				}
				MetaData* tempMetaData = metaData;
				while (tempMetaData->NextPageNo) {
					const uchar* tempStart =
							TempMMapBuffer::getInstance().getAddress()
									+ metaData->NextPageNo
											* MemoryBuffer::pagesize;
					tempMetaData = (MetaData*) tempStart;
					const uchar* tempEnd = tempStart + tempMetaData->usedSpace;

					tempStart += sizeof(MetaData);
					while (tempStart < tempEnd) {
						tempStart = Chunk::readID(tempStart, subjectID);
						tempStart = Chunk::read(tempStart, objType, CHAR);
						tempStart = Chunk::read(tempStart, object, objType);
						sp << "chunk," << chunkId << "," << subjectID << ","
								<< meta->pid << "," << object << endl;
					}

					tempStart = tempEnd - tempMetaData->usedSpace
							+ MemoryBuffer::pagesize;

				}
				if (isFirstPage) {
					bufferReader = endPtr - metaData->usedSpace
							- sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
					isFirstPage = false;
				} else {
					bufferReader = endPtr - metaData->usedSpace
							+ MemoryBuffer::pagesize;
				}
				chunkId++;
			}

		} else if (meta->soType == ORDERBYO) {
			const uchar* endChunkManager = bufferReader
					- sizeof(ChunkManagerMeta) + meta->length;
			bool isFirstPage = true;
			int chunkId = 0;
			while (bufferReader < endChunkManager) {
				MetaData* metaData = (MetaData*) bufferReader;
				//op << "ORDERBYO: " <<  meta->pid << "\t" << "chunk:" << chunkId << "\t" << metaData->usedSpace << "\t" << metaData->min << "\t" << metaData->max << endl;
				const uchar* endPtr = bufferReader + metaData->usedSpace;
				bufferReader += sizeof(MetaData);
				while (bufferReader < endPtr) {
					bufferReader = Chunk::read(bufferReader, objType, CHAR);
					bufferReader = Chunk::read(bufferReader, object, objType);
					bufferReader = Chunk::readID(bufferReader, subjectID);
					op << "chunk," << chunkId << "," << subjectID << ","
							<< meta->pid << "," << object << endl;
				}

				MetaData* tempMetaData = metaData;
				while (tempMetaData->NextPageNo) {
					const uchar* tempStart =
							TempMMapBuffer::getInstance().getAddress()
									+ metaData->NextPageNo
											* MemoryBuffer::pagesize;
					tempMetaData = (MetaData*) tempStart;
					const uchar* tempEnd = tempStart + tempMetaData->usedSpace;

					tempStart += sizeof(MetaData);
					while (tempStart < tempEnd) {
						tempStart = Chunk::read(tempStart, objType, CHAR);
						tempStart = Chunk::read(tempStart, object,
								objType);
						tempStart = Chunk::readID(tempStart, subjectID);
						op << "chunk," << chunkId << "," << subjectID << ","
								<< meta->pid << "," << object << endl;
					}

					tempStart = tempEnd - tempMetaData->usedSpace
							+ MemoryBuffer::pagesize;

				}

				if (isFirstPage) {
					bufferReader = endPtr - metaData->usedSpace
							- sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
					isFirstPage = false;
				} else {
					bufferReader = endPtr - metaData->usedSpace
							+ MemoryBuffer::pagesize;
				}
				chunkId++;
			}
		}
	}

	sp.close();
	op.close();

	cout << "over" << endl;
	return 0;
}
