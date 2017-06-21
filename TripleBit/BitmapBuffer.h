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
#include "LineHashIndex.h"
#include "ThreadPool.h"

///////////////////////////////////////////////////////////////////////////////////////////////
///// class BitmapBuffer
///////////////////////////////////////////////////////////////////////////////////////////////
class BitmapBuffer {
public:
	map<ID, ChunkManager*> predicate_managers[2];//基于subject和object排序的每个predicate对应的ChunkManager
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
	Status insertPredicate(ID predicateID, OrderByType soType);
	//插入一条三元组信息，根据object数据类型确定插入object所占字节
	Status insertTriple(ID predicateID, ID subjectID, double object, OrderByType soType, char objType = STRING);
	//根据predicate与SO排序方式获取对应的ChunkManager
	ChunkManager* getChunkManager(ID predicateID, OrderByType soType);
	//获取数据库中所有三元组总数
	size_t getTripleCount();
	//根据SO排序类型增大临时文件一个Chunk大小，并修改临时文件中ChunkManagerMeta的尾指针地址，返回添加Chunk的初始地址
	uchar* getPage(bool soType, size_t& pageNo);
	//将缓冲区文件写入文件中
	void flush();
	//将临时文件数据存储到数据库三元组存储文件BitmapBuffer中，建立谓词相关信息
	void save();

	void endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld);

};

/////////////////////////////////////////////////////////////////////////////////////////////
///////// class ChunkManager
/////////////////////////////////////////////////////////////////////////////////////////////
struct ChunkManagerMeta
{
	uint pid;//predicate id
	bool soType;// SO排序类型,false:s;true:o
	size_t length;//已申请的Chunk空间
	size_t usedSpace;//数据区已使用空间，包括MetaData头部信息，不包含ChunkManagerMeta存储空间
	DataType objType;//predicate对应的object数据类型
	int tripleCount;//该ChunkManager中triple的总数
	uchar* startPtr;//该ChunkManager中数据区的起始位置，即每个Chunk中MetaData的位置
	uchar* endPtr;//该ChunkManager中数据区的结束位置
};

struct MetaData
{
	size_t pageNo;
	double min, max;//以S（O）排序的Chunk中以S（O）的最小值与最大值
	size_t usedSpace;//Chunk已使用空间大小
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
	ChunkManager(ID predicateID, OrderByType soType, BitmapBuffer* _bitmapBuffer);
	~ChunkManager();
	//为Chunk建立索引信息
	Status buildChunkIndex();
	//更新Chunk索引信息
	Status updateChunkIndex();
	//在chunk中插入数据x，y, x表示subjectID， y表示object
	void insertXY(ID x, double y, char objType = STRING);
	//向指定位置写入数据x，y，写完后指针仍指向原地址, x表示subjectID， y表示object
	void writeXY(const uchar* reader, ID x, double y, char objType = STRING);
	//根据数据类型删除在指定位置数据，返回删除后位置，删除将该位置0
	uchar* deleteTriple(uchar* reader, char objType = STRING);
	//获取新的Chunk
	Status resize(size_t& pageNo);
	//判断添加len长度数据后Chunk是否溢出
	bool isChunkOverFlow(uint len);
	//获取Chunk总数
	size_t getChunkNumber();
	//更新triple数量
	Status tripleCountAdd() {
		meta->tripleCount++;
		return OK;
	}

	ChunkManagerMeta* getChunkManagerMeta(){
		return meta;
	}

	LineHashIndex* getChunkIndex() {
		return chunkIndex;
	}
	int getTripleCount() {
		return meta->tripleCount;
	}
	ID getPredicateID() const {
		return meta->pid;
	}
	uchar* getStartPtr() {
		return meta->startPtr;
	}
	uchar* getEndPtr() {
		return meta->endPtr;
	}
	void setMetaDataMin(MetaData *metaData, ID x, double y);
	//加载ChunkManager相关信息
	static ChunkManager* load(ID predicateID, bool soType, uchar* buffer, size_t& offset);

};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class Chunk {
public:
	Chunk();
	~Chunk();

	//在指定位置写入ID数据，默认返回写后数据位置
	static void writeID(const uchar*& writer, ID data, bool isUpdateAdress = true);
	//在指定位置根据数据类型写入数据，默认返回写后数据位置
	template<typename T>
	static void write(const uchar*& writer, T data, char dataType = STRING, bool isUpdateAdress = true);
	//在指定位置读取ID数据，默认返回读取后数据位置
	static const uchar* readID(const uchar* reader, ID& data, bool isUpdateAdress = true);
	//在指定位置根据数据类型读取数据，默认返回读取后数据位置
	template<typename T>
	static const uchar* read(const uchar* reader, T& data, char dataType = STRING);
	//根据数据类型删除在指定位置数据，返回删除后位置，删除将该位置0
	static uchar* deleteData(uchar* reader, char objType = STRING);
	///根据object数据类型获取一对数据的字节长度
	static uint getLen(char dataType = STRING);
	//根据objType判定是否有数据存储、已删除、无数据,若有数据或数据已删除则返回数据类型或已删除数据类型, 并将修改reader指向地址为该条数据object后面的地址
	static Status getObjTypeStatus(const uchar*& reader, uint& moveByteNum);
	/// Skip a s or o
	static const uchar* skipData(const uchar* reader, char dataType = STRING);
	//根据object数据类型从reader位置向前跳至第一对x-y值
	static const uchar* skipForward(const uchar* reader, const uchar* endPtr, OrderByType soType);
	//根据object数据类型在endPtr位置向后跳至最后一对x-y值, reader为MetaData位置
	static const uchar* skipBackward(const uchar* reader, const uchar* endPtr, OrderByType soType);
};
#endif /* CHUNKMANAGER_H_ */
