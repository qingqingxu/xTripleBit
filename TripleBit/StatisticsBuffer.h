/*
 * StatisticsBuffer.h
 *
 *  Created on: Aug 31, 2010
 *      Author: root
 */

#ifndef STATISTICSBUFFER_H_
#define STATISTICSBUFFER_H_

class HashIndex;
class EntityIDBuffer;
class MMapBuffer;

#include "TripleBit.h"

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
	Status addStatis(T soValue, ID predicateID, size_t count, char objType = STRING);
	//获取一条SP或OP的统计信息
	template<typename T>
	Status getStatis(T soValue, ID predicateID, size_t& count, char objType = STRING);
	//根据SP（OP）统计信息获取S（O）出现的次数
	template<typename T>
	Status getStatisBySO(T soValue, size_t& count, char objType = STRING);
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
