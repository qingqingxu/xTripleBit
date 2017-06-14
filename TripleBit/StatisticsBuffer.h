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

class StatisticsBuffer {
public:
	enum StatisticsType { SUBJECTPREDICATE_STATIS, OBJECTPREDICATE_STATIS };
	StatisticsBuffer();
	virtual ~StatisticsBuffer();
	/// add a statistics record;
	virtual Status addStatis(unsigned v1, unsigned v2, unsigned v3) = 0;
	/// get a statistics record;
	virtual Status getStatis(unsigned& v1, unsigned v2) = 0;
	/// save the statistics record to file;
	//virtual Status save(ofstream& file) = 0;
	/// load the statistics record from file;
	//virtual StatisticsBuffer* load(ifstream& file) = 0;
protected:
	const unsigned HEADSPACE;
};

class TwoConstantStatisticsBuffer : public StatisticsBuffer {
public:
	struct Triple{
		ID value1;
		ID value2;
		ID count;
	};

private:
	StatisticsType type;
	MMapBuffer* buffer;
	const unsigned char* reader;
	unsigned char* writer;

	Triple* index;

	unsigned lastId, lastPredicate;
	unsigned usedSpace;
	unsigned currentChunkNo;
	unsigned indexPos, indexSize;

	Triple triples[3 * 4096];
	Triple* pos, *posLimit;
	bool first;
public:
	TwoConstantStatisticsBuffer(const string path, StatisticsType type);
	virtual ~TwoConstantStatisticsBuffer();
	/// add a statistics record;
	Status addStatis(unsigned v1, unsigned v2, unsigned v3);
	/// get a statistics record;
	Status getStatis(unsigned& v1, unsigned v2);
	/// get the buffer position by a id, used in query
	Status getPredicatesByID(unsigned id, EntityIDBuffer* buffer, ID minID, ID maxID);
	/// save the statistics buffer;
	Status save(MMapBuffer*& indexBuffer);
	/// load the statistics buffer;
	static TwoConstantStatisticsBuffer* load(StatisticsType type, const string path, uchar*& indxBuffer);
private:
	/// decode a chunk
	const uchar* decode(const uchar* begin, const uchar* end);
	///
	bool findPriorityByValue1(unsigned value1, unsigned value2);
	bool findPriorityByValue2(unsigned value1, unsigned value2);
	int findPredicate(unsigned,Triple*,Triple*);
	///
	bool find_last(unsigned value1, unsigned value2);
	bool find(unsigned,Triple* &,Triple* &);
	const uchar* decode(const uchar* begin, const uchar* end,Triple*,Triple*& ,Triple*&);

};
#endif /* STATISTICSBUFFER_H_ */
