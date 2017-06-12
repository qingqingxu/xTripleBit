/*
 * ChunkManager.h
 *
 *  Created on: 2010-4-12
 *      Author: liupu
 */

#ifndef CHUNKMANAGER_H_
#define CHUNKMANAGER_H_

class MemoryBuffer;
class MMapBuffer;
class Chunk;
class ChunkManager;

#include "TripleBit.h"
#include "HashIndex.h"
#include "LineHashIndex.h"
#include "ThreadPool.h"

///////////////////////////////////////////////////////////////////////////////////////////////
///// class BitmapBuffer
///////////////////////////////////////////////////////////////////////////////////////////////
class BitmapBuffer {
public:
	map<ID, ChunkManager*> predicate_managers[2];//基于subject和object排序的每个predicate对应的ChunkManager
	map<ID, DataType> predicateObjTypes;
	const string dir;//位图矩阵存储的路径
	MMapBuffer *tempByS, *tempByO;//存储以subject或object排序的ID编码后三元组信息
	size_t usedPageByS, usedPageByO;//以subject或object排序存储三元组已使用的Chunk数量
public:
	BitmapBuffer(){}
	BitmapBuffer(const string dir);
	~BitmapBuffer();
	//加载predicate对应的ChunkManager信息与对应的索引信息
	static BitmapBuffer* load(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage);
	//插入一条predicate信息，创建以subject和以object排序的ChunkManager，并确认predicate对应object数据类型
	template<typename T>
	Status insertPredicate(T& predicate, OrderByType soType, DataType objType = DataType::STRING);
	//插入一条三元组信息，根据object数据类型确定插入object所占字节
	template<typename T>
	Status insertTriple(T& predicate, T& subject, T& object, DataType objType = DataType::STRING);
	//根据predicate与SO排序方式获取对应的ChunkManager
	template<typename T>
	ChunkManager* getChunkManager(T& predicate, OrderByType soType);
	//获取数据库中所有三元组总数
	size_t getTripleCount();
	//根据SO排序类型增大临时文件一个Chunk大小，并修改临时文件中ChunkManagerMeta的尾指针地址，返回添加Chunk的初始地址
	uchar* getPage(OrderByType soType, size_t& pageNo);
	//将缓冲区文件写入文件中
	void flush();
	//将临时文件数据存储到数据库三元组存储文件BitmapBuffer中，建立谓词相关信息
	void save();


	/// insert a predicate given specified sorting type and predicate id
	Status insertPredicate(ID predicateID, unsigned char typeID);//n
	/// insert a triple;
	Status insertTriple(ID predicateID, ID xID, ID yID, bool isBigger, unsigned char typeID);//n
	void endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld);

};

/////////////////////////////////////////////////////////////////////////////////////////////
///////// class ChunkManager
/////////////////////////////////////////////////////////////////////////////////////////////
struct ChunkManagerMeta
{
	unsigned pid;//predicate id
	OrderByType soType;// SO排序类型
	size_t length;//已申请的Chunk空间
	size_t usedSpace;//数据区已使用空间，包括MetaData头部信息，不包含ChunkManagerMeta存储空间
	DataType objType;//predicate对应的object数据类型
	DataType xType;//存储x值对应数据类型
	DataType yType;//存储y值对应数据类型
	int tripleCount;//该ChunkManager中triple的总数
	uchar* startPtr;//该ChunkManager中数据区的起始位置，即每个Chunk中MetaData的位置
	uchar* endPtr;//该ChunkManager中数据区的结束位置
};

struct MetaData
{
	bool minBool, maxBool;//以S（O）排序的Chunk中以S（O）的最小值与最大值
	char minChar, maxChar;
	ID minID, maxID;
	double minDouble, maxDouble;
	size_t usedSpace;//Chunk已使用空间大小
	bool haveNextPage;//该Chunk是否还有后续Chunk
	ulonglong nextChunkDiff; //下一Chunk地址距离当前Chunk到距离  ?
	size_t NextPageNo;//下一个Chunk的Chunk号
};

class ChunkManager {
private:
	ChunkManagerMeta* meta;//每个ChunkManager的元数据信息
	BitmapBuffer* bitmapBuffer;//BitmapBuffer
	LineHashIndex* chunkIndex;//以S（O）排序存储的每个predicate存储块的索引信息
	uchar* lastChunkStartPtr;//以S（O）排序存储的每个predicate存储块最后一个Chunk的起始位置
	vector<size_t> usedPages;//ChunkManager所占用的页号集合
public:
	friend class BuildSortTask;
	friend class BuildMergeTask;
	friend class HashIndex;
	friend class BitmapBuffer;
	friend class PartitionMaster;

public:
	ChunkManager(){}
	template<typename T>
	ChunkManager(T predicate, OrderByType soType, DataType objType, BitmapBuffer* _bitmapBuffer);
	~ChunkManager();
	//加载ChunkManager相关信息
	template<typename T>
	static ChunkManager* load(T& predicate, OrderByType soType, uchar* buffer, size_t& offset);
	//为Chunk建立索引信息
	Status buildChunkIndex();
	//更新Chunk索引信息
	Status updateChunkIndex();
	//在chunk中插入数据x，y
	template<typename T>
	void insertXY(T& x, T& y);
	//向指定位置写入数据x，y，写完后指针仍指向原地址
	template<typename T>
	void writeXY(const uchar* reader, T& x, T& y);
	//获取新的Chunk
	Status resize();
	//判断添加len长度数据后Chunk是否溢出
	bool isChunkOverFlow(uint len);
	//获取Chunk总数
	size_t getChunkNumber();
	//更新triple数量
	Status tripleCountAdd() {
		meta->tripleCount++;
		return OK;
	}

	LineHashIndex* getChunkIndex() {
		return chunkIndex;
	}
	int getTripleCount() {
		return meta->tripleCount;
	}
	template<typename T>
	T getPredicate() const {
		return meta->pid;
	}

	int getTripleCount(uchar type) {
			return meta->tripleCount[type - 1];
	}
	unsigned int getPredicateID() const {//n
		return meta->pid;
	}



	size_t getChunkNumber(uchar type);

	void insertXY(unsigned x, unsigned y, unsigned len, uchar type);//n

	uchar* getStartPtr(uchar type) {
		return meta->startPtr[type -1];
	}

	uchar* getEndPtr(uchar type) {
		return meta->endPtr[type -1];
	}

	Status buildChunkIndex();
	Status updateChunkIndex();
	static ChunkManager* load(unsigned pid, unsigned type, uchar* buffer, size_t& offset);//n
private:
	template<typename T>
	void setMetaDataMin(MetaData *metaData, T t);
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

#include <boost/dynamic_bitset.hpp>

class Chunk {
public:
	Chunk();
	~Chunk();
	//在指定位置根据数据类型写入数据，返回写后数据位置
	template<typename T>
	static void write(uchar*& writer, T& data, DataType dataType = DataType::STRING);
	//在指定位置根据数据类型读取数据，返回读取后数据位置
	template<typename T>
	static const uchar* read(const uchar* reader, T& data, DataType dataType = DataType::STRING);
	//根据数据类型删除在指定位置数据，返回删除后位置，删除将该位置0
	template<typename T>
	static uchar* deleteData(uchar* reader, DataType dataType = DataType::STRING);
	/// Skip a s or o
	static const uchar* skipId(const uchar* reader, DataType dataType = DataType::STRING);
	//根据object数据类型向前跳过一对x-y值
	static const uchar* skipForward(const uchar* reader, DataType objType);
	//根据object数据类型向前跳过一对x-y值
	static const uchar* skipBackward(const uchar* reader, const uchar* endPtr);




	/// Read a subject id
	static const uchar* readXId(const uchar* reader, register ID& id);
	/// Read an object id
	static const uchar* readYId(const uchar* reader, register ID& id);

	/// Delete a subject id (just set the id to 0)
	static uchar* deleteXId(uchar* reader);
	/// Delete a object id (just set the id to 0)
	static uchar* deleteYId(uchar* reader);
	/// Skip a s or o
	static const uchar* skipId(const uchar* reader, unsigned char idNums);
	/// Skip backward to s
	static const uchar* skipBackward(const uchar* reader);
	static const uchar* skipBackward(const uchar* reader, const uchar* endPtr, uint step);
	static const uchar* skipBackward(const uchar* reader, uint step, bool isFirstPage);


};
#endif /* CHUNKMANAGER_H_ */
