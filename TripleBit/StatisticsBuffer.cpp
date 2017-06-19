/*
 * StatisticsBuffer.cpp
 *
 *  Created on: Aug 31, 2010
 *      Author: root
 */

#include "StatisticsBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "HashIndex.h"
#include "EntityIDBuffer.h"
#include "URITable.h"
#include "MemoryBuffer.h"

//#define MYDEBUG

template<typename T>
extern uchar* writeData(uchar* writer, T data);
template<typename T>
extern const uchar* readData(const uchar* reader, T& data);

StatisticsBuffer::StatisticsBuffer(const string path, StatisticsType statType) :statType(statType) {
	buffer = new MMapBuffer(path.c_str(),
			STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	writer = (uchar*) buffer->getBuffer();
	usedSpace = 0;
	indexPos = 0;
	indexSize = 0; //MemoryBuffer::pagesize;
	index = NULL;
	first = true;
}

StatisticsBuffer::~StatisticsBuffer() {
	if (buffer != NULL) {
		delete buffer;
	}
	buffer = NULL;
	if (index != NULL) {
		delete index;
	}
	index = NULL;
}

template<typename T>
Status StatisticsBuffer::addStatis(T soValue, ID predicateID, size_t count,
		char objType) {
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

/**
 * 获取SP或OP的统计信息
 * SP：直接解压SP范围内的SP进行比较
 * OP：除了OP比较外，还需比较O的数据类型
 */
void StatisticsBuffer::decodeStatis(const uchar* begin, const uchar* end, double soValue, ID predicateID, size_t& count, char objType) {
	ID tempPredicateID;
	size_t tempCount;
	if (statType == SUBJECTPREDICATE_STATIS) {
		ID subjectID;
		while (begin + sizeof(ID) < end) {
			begin = readData(begin, subjectID);
			if (subjectID && begin + sizeof(ID) < end) {
				begin = readData(begin, tempPredicateID);
				if (tempPredicateID && begin + sizeof(size_t) <= end) {
					begin = readData(begin, tempCount);
					if(subjectID == soValue && tempPredicateID == predicateID){
						count = tempCount;
						return;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	} else if (statType == OBJECTPREDICATE_STATIS) {
		char tempObjType;
		double tempObject;
		uint moveByteNum;
		int status;
		while (begin + sizeof(char) < end) {
			status = Chunk::getObjTypeStatus(begin, moveByteNum);
			if (status == DATA_EXSIT) {
				begin -= moveByteNum;
				begin = readData(begin, tempObjType);
				if (begin + Chunk::getLen(tempObjType) < end) {
					begin = Chunk::read(begin, tempObject, tempObjType);
					if (begin + sizeof(ID) < end) {
						begin = readData(begin, tempPredicateID);
						if (tempPredicateID && begin + sizeof(size_t) <= end) {
							begin = readData(begin, tempCount);
							if(tempObject == soValue && tempPredicateID == predicateID && objType == tempObjType){
								count = tempCount;
								return;
							}
						} else {
							break;
						}
					} else {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}
}

void StatisticsBuffer::decodeStatis(const uchar* begin, const uchar* end, double soValue, size_t& count, char objType){
	ID predicateID;
	size_t tempCount = 0;
		if (statType == SUBJECTPREDICATE_STATIS) {
			ID subjectID;
			while (begin + sizeof(ID) < end) {
				begin = readData(begin, subjectID);
				if (subjectID && begin + sizeof(ID) < end) {
					begin = readData(begin, predicateID);
					if (predicateID && begin + sizeof(size_t) <= end) {
						begin = readData(begin, tempCount);
						if(subjectID == soValue){
							count += tempCount;
						}else if(subjectID > soValue){
							break;
						}
					} else {
						break;
					}
				} else {
					break;
				}
			}
		} else if (statType == OBJECTPREDICATE_STATIS) {
			char tempObjType;
			double object;
			uint moveByteNum;
			int status;
			while (begin + sizeof(char) < end) {
				status = Chunk::getObjTypeStatus(begin, moveByteNum);
				if (status == DATA_EXSIT) {
					begin -= moveByteNum;
					begin = readData(begin, tempObjType);
					if (begin + Chunk::getLen(objType) < end) {
						begin = Chunk::read(begin, object, objType);
						if (begin + sizeof(ID) < end) {
							begin = readData(begin, predicateID);
							if (predicateID && begin + sizeof(size_t) <= end) {
								begin = readData(begin, tempCount);
								if(soValue == object && objType == tempObjType){
									count += tempCount;
								}else if(soValue > object){
									break;
								}
							} else {
								break;
							}
						} else {
							break;
						}
					} else {
						break;
					}
				} else {
					break;
				}
			}
		}
}

static inline bool greater(ID a1, double a2, ID b1, double b2) {
	return (a1 > b1) || ((a1 == b1) && (a2 > b2));
}

static inline bool less(double a1, ID a2, double b1, ID b2) { //useless
	return (a1 < b1) || ((a1 == b1) && (a2 < b2));
}

bool StatisticsBuffer::findLocation(ID predicateID, double soValue) {
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);

		if (::greater(predicateID, soValue, pos[middle].predicateID,
				pos[middle].soValue)) {
			left = middle + 1;
		} else if ((!middle)
				|| ::greater(predicateID, soValue, pos[middle - 1].predicateID,
						pos[middle - 1].soValue)) {
			break;
		} else {
			right = middle;
		}
	}

	if (left == right) {
		return false;
	} else {
		pos = &pos[middle]; // value1 and value2 is between middle-1 and middle
		return true;
	}
}

bool StatisticsBuffer::findLocation(double soValue) {
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);

		if (soValue > pos[middle].soValue) {
			left = middle + 1;
		} else if ((!middle) || soValue > pos[middle - 1].soValue) {
			break;
		} else {
			right = middle;
		}
	}

	if (left == right) {
		return false;
	} else {
		pos = &pos[middle]; // value1 and value2 is between middle-1 and middle
		return true;
	}
}

template<typename T>
Status StatisticsBuffer::getStatis(T soValue, ID predicateID,
		size_t& count, char objType) {
	pos = index, posLimit = index + indexPos;
	findLocation(predicateID, soValue); // get index location, that is pos
	if (::greater(pos->predicateID, pos->soValue, predicateID, soValue))
		pos--;

	uint start = pos->count;
	while(pos <= posLimit && !greater(pos->predicateID, pos->soValue, predicateID, soValue)){
		pos++;
	}
	uint end = pos->count; // count is usedspace
	if (pos == (index + indexPos))
		end = usedSpace;

	const uchar* begin = (uchar*) buffer->getBuffer() + start, *limit =
			(uchar*) buffer->getBuffer() + end;
	decodeStatis(begin, limit, soValue, predicateID, count, objType);
	findLocation(predicateID, soValue);
	if (count) {
		return OK;
	}

	return NOT_FOUND;
}

template<typename T>
Status StatisticsBuffer::getStatisBySO(T soValue, size_t& count, char objType) {
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

Status StatisticsBuffer::save(MMapBuffer*& indexBuffer) {
	uchar* writer;
	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(
				string(string(DATABASE_PATH) + "/statIndex").c_str(),
				indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize(indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address() + size;
	}

	writer = writeData(writer, usedSpace);
	writer = writeData(writer, indexPos);

	memcpy(writer, (char*) index, indexPos * sizeof(Triple));
	free(index);

	return OK;
}

StatisticsBuffer* StatisticsBuffer::load(StatisticsType statType, const string path, uchar*& indexBuffer) {
	StatisticsBuffer* statBuffer = new StatisticsBuffer(path, statType);

	indexBuffer = (uchar*) readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (uchar*) readData(indexBuffer, statBuffer->indexPos);
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	statBuffer->index = (Triple*) indexBuffer;
	indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);

	return statBuffer;
}
