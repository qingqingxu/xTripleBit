/*
 * StatisticsBuffer.h
 *
 *  Created on: Aug 31, 2010
 *      Author: root
 */

#ifndef STATISTICSBUFFER_H_
#define STATISTICSBUFFER_H_



#include "TripleBit.h"
#include "MemoryBuffer.h"
#include "MMapBuffer.h"

class MMapBuffer;

//SP、OP统信息类，存储结构：s-p-spcount、o-p-opcount

class StatisticsBuffer{
public:
	struct Triple{
		double soValue;
		ID predicateID;
		uint count;
	};
private:
	StatisticsType statType;
	MMapBuffer* buffer;
	uchar* writer;

	Triple* index;

	uint usedSpace;
	uint indexPos, indexSize;

	Triple triples[3 * 4096];
	Triple* pos, *posLimit;
	bool first;
public:
	StatisticsBuffer(const string path, StatisticsType statType);
	~StatisticsBuffer();
	//插入一条SP或OP的统计信息,加入OP统计信息需指定objType
	template<typename T>
	Status addStatis(T soValue, ID predicateID, size_t count, char objType = STRING){
		unsigned len = sizeof(T) + sizeof(ID) + sizeof(sizeof(size_t));

			if (first || usedSpace + len > buffer->getSize()) {
				usedSpace = writer - (uchar*) buffer->getBuffer();
				buffer->resize(
						STATISTICS_BUFFER_INCREMENT_PAGE_COUNT
								* MemoryBuffer::pagesize);	//加大空间
				writer = (uchar*) buffer->getBuffer() + usedSpace;

				if ((indexPos + 1) >= indexSize) {
		#ifdef DEBUF
					cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
		#endif
					index = (Triple*) realloc(index,
							indexSize * sizeof(Triple)
									+ MemoryBuffer::pagesize * sizeof(Triple));
					if (index == NULL) {
						cout << "realloc StatisticsBuffer error" << endl;
						return ERR;
					}
					indexSize += MemoryBuffer::pagesize;
				}

				index[indexPos].soValue = soValue;
				index[indexPos].predicateID = predicateID;
				index[indexPos].count = usedSpace; //record offset，可以得出实体——谓词所在的块号

				indexPos++;
				first = false;
			}

			if (statType == SUBJECTPREDICATE_STATIS) {

			} else if (statType == OBJECTPREDICATE_STATIS) {
				writer = writeData(writer, objType); //OP统计信息O前需加objType
			}
			writer = writeData(writer, soValue);
			writer = writeData(writer, predicateID);
			writer = writeData(writer, count);

			usedSpace = writer - (uchar*) buffer->getBuffer();
			return OK;
	}
	//获取一条SP或OP的统计信息
	Status getStatis(double soValue, ID predicateID, size_t& count, char objType = STRING);
	//根据SP（OP）统计信息获取S（O）出现的次数
	template<typename T>
	Status getStatisBySO(T soValue, size_t& count, char objType = STRING){
		count = 0;
			pos = index, posLimit = index + indexPos;
			findLocation(soValue);
			if (pos->soValue >= soValue) {
				pos--;
			}

			uint start = pos->count;
			while (pos <= posLimit && pos->soValue <= soValue) {
				pos++;
			}

			uint end = pos->count; // count is usedspace
			if (pos == (index + indexPos))
				end = usedSpace;

			const uchar* begin = (uchar*) buffer->getBuffer() + start, *limit =
					(uchar*) buffer->getBuffer() + end;
			decodeStatis(begin, limit, soValue, count, objType);

			if (count != 0) {
				return OK;
			}

			return NOT_FOUND;
	}
	//定位SP或OP初始位置,pos: the start address of the first triple;posLimit: the end address of last triple;
	bool findLocation(ID predicateID, double soValue);
	//定位S或O初始位置,pos: the start address of the first triple;posLimit: the end address of last triple;
	bool findLocation(double soValue);
	//建立统计信息的索引
	Status save(MMapBuffer*& indexBuffer);
	//加载统计信息的索引
	static StatisticsBuffer* load(StatisticsType statType, const string path, uchar*& indxBuffer);
private:
	/// decode a statistics chunk
	void decodeStatis(const uchar* begin, const uchar* end, double soValue, ID predicateID, size_t& count, char objType = STRING);
	void decodeStatis(const uchar* begin, const uchar* end, double soValue, size_t & count, char objType = STRING);
};
#endif /* STATISTICSBUFFER_H_ */
