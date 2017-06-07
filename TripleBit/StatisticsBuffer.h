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
	enum StatisticsType { SUBJECT_STATIS, OBJECT_STATIS, SUBJECTPREDICATE_STATIS, OBJECTPREDICATE_STATIS };
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

class OneConstantStatisticsBuffer : public StatisticsBuffer {
public:
	struct Triple {
		ID value1;
		unsigned count;
	};

private:
	StatisticsType type;
	MMapBuffer* buffer;
	const uchar* reader;
	uchar* writer;

	/// index for query;
	vector<unsigned> index;
	unsigned indexSize;
	unsigned nextHashValue;
	unsigned lastId;
	unsigned usedSpace;

	const unsigned ID_HASH;

	Triple* triples, *pos, *posLimit;
	bool first;
public:
	OneConstantStatisticsBuffer(const string path, StatisticsType type);
	virtual ~OneConstantStatisticsBuffer();
	Status addStatis(unsigned v1, unsigned v2, unsigned v3 = 0);
	Status getStatis(unsigned& v1, unsigned v2 = 0);
	Status save(MMapBuffer*& indexBuffer);
	static OneConstantStatisticsBuffer* load(StatisticsType type, const string path, uchar*& indexBuffer);
	/// get the subject or object ids from minID to maxID;
	Status getIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID);
	unsigned int getEntityCount();
private:
	/// read a id from buffer;
	const char* readId(unsigned& id, const char* ptr, bool isID);
	/// judge the buffer is full;
	bool isPtrFull(unsigned len);

	const unsigned char* decode(const unsigned char* begin, const unsigned char* end);
	bool find(unsigned value);
	bool find_last(unsigned value);
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
