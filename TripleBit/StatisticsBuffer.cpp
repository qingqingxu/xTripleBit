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

#define MYDEBUG

extern uchar* writeData(uchar* writer, uint data);
extern const char* readData(const uchar* reader, uint& data);

StatisticsBuffer::StatisticsBuffer() : HEADSPACE(2) {
	// TODO Auto-generated constructor stub
	
}

StatisticsBuffer::~StatisticsBuffer() {
	// TODO Auto-generated destructor stub
}

/////////////////////////////////////////////////////////////////

OneConstantStatisticsBuffer::OneConstantStatisticsBuffer(const string path, StatisticsType type) : StatisticsBuffer(), type(type), reader(NULL), ID_HASH(50)
{
	buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	writer = (unsigned char*)buffer->getBuffer();
//	reader = NULL;
	index.resize(2000);
	nextHashValue = 0;
	lastId = 0;
	usedSpace = 0;
	reader = NULL;

	triples = new Triple[ID_HASH];
	first = true;
}

OneConstantStatisticsBuffer::~OneConstantStatisticsBuffer()
{
	if(buffer != NULL) {
		delete buffer;
	}

	if(triples != NULL) {
		delete[] triples;
		triples = NULL;
	}
	buffer = NULL;
}

bool OneConstantStatisticsBuffer::isPtrFull(unsigned len) 
{
	return (uint) ( writer - (uchar*)buffer->getBuffer() + len ) > buffer->getSize() ? true : false;
}

const unsigned char* OneConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end)
{
	Triple* writer = triples;
	unsigned value1, count;
	readData(begin, value1);
	while(begin < end && value1){
		begin = readData(begin, value1);
		begin = readData(begin, count);

		(*writer).value1 = value1;
		(*writer).count = count;
		writer++;

		readData(begin, value1);
	}

	pos = triples;
	posLimit = writer;

	return begin;
}

unsigned int OneConstantStatisticsBuffer::getEntityCount()
{
	unsigned int entityCount = 0;
	unsigned i = 0;

	const unsigned char* begin, *end;
	unsigned beginChunk = 0, endChunk = 0;

#ifdef DEBUG
	cout<<"indexSize: "<<indexSize<<endl;
#endif
	for(i = 1; i <= indexSize; i++) {
		if(i < indexSize)
			endChunk = index[i];

		while(endChunk == 0 && i < indexSize) {
			i++;
			endChunk = index[i];
		}
		
		if(i == indexSize) { 
			endChunk = usedSpace;
		}
			
		if(endChunk != 0) {
			begin = (const uchar*)(buffer->getBuffer()) + beginChunk;
			end = (const uchar*)(buffer->getBuffer()) + endChunk;
			entityCount = entityCount + (end - begin) / (4 * 2);

			beginChunk = endChunk;
		}
	}

	return entityCount;
}

Status OneConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3 /* = 0 */)
{
/*
#ifdef MYDEBUG
	cout << "OneConstantStatisticsBuffer: " << v1 << "\t" << v2 << "\t" << v3 << endl;
#endif
*/
	unsigned len = 4 * 2;
	if (isPtrFull(len) == true) {
		usedSpace = writer - (uchar*) buffer->getBuffer();
		buffer->resize(
				STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
		writer = (uchar*) buffer->getBuffer() + usedSpace;
/*
#ifdef MYDEBUG
	cout << "OneConstantStatisticsBuffer: " << buffer->getSize() << endl;
#endif
*/
	}

	if (first || v1 >= nextHashValue) {
		unsigned offset = writer - (uchar*) buffer->getBuffer();
		while (index.size() <= (v1 / ID_HASH)) {
			index.resize(index.size() + 2000, 0);//vector<unsigned> index;其大小每次充满时便添加2000
#ifdef MYDEBUG
			cout << "index size" << index.size() << " v1 / ID_HASH: "
			<< (v1 / ID_HASH) << endl;
#endif
		}
		index[v1 / ID_HASH] = offset; // (v1/ID_HASH),得到在hash索引中的偏移值，从而得到ID所在块号
		while (nextHashValue <= v1)
			nextHashValue += HASH_RANGE;
		first = false;
	}

	writer = writeData(writer, v1);
	writer = writeData(writer, v2);

	usedSpace = writer - (uchar*) buffer->getBuffer();

	return OK;
}

bool OneConstantStatisticsBuffer::find(unsigned value)
{
	//const Triple* l = pos, *r = posLimit;
	int l = 0, r = posLimit - pos;
	int m;
	while (l != r) {
		m = l + ((r - l) / 2);
		if (value > pos[m].value1) {
			l = m + 1;
		} else if ((!m) || value > pos[m - 1].value1) {
			break;
		} else {
			r = m;
		}
	}

	if(l == r)
		return false;
	else {
		pos = &pos[m];
		return true;
	}

}

bool OneConstantStatisticsBuffer::find_last(unsigned value)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle = 0;

	while (left < right) {
		middle = left + ((right - left) / 2);
		if (value < pos[middle].value1) {
			right = middle;
		} else if ((!middle) || value < pos[middle + 1].value1) {
			break;
		} else {
			left = middle + 1;
		}
	}

	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

Status OneConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2 /* = 0 */)
{
	unsigned i;
	unsigned begin = index[ v1 / ID_HASH];
	unsigned end = 0;

	i = v1 / ID_HASH + 1;
	while(i < indexSize) { // get next chunk start offset;
		if(index[i] != 0) {
			end = index[i];
			break;
		}
		i++;
	}

	if(i == indexSize)
		end = usedSpace;

	reader = (uchar*)buffer->getBuffer() + begin;

	readData(reader, lastId);
	reader += 4;

	//reader = readId(lastId, reader, true);
	if ( lastId == v1 ) {
		//reader = readId(v1, reader, false);
		readData(reader, v1);
		return OK;
	}

	const uchar* limit = (uchar*)buffer->getBuffer() + end;
	this->decode(reader - 4, limit);
	if(this->find(v1)) {
		if(pos->value1 == v1) {
			v1 = pos->count;
			return OK;
		}
	}
	v1 = 0;
	return ERROR;
}

Status OneConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer)
{
#ifdef DEBUG
	cout<<"index size: "<<index.size()<<endl;
#endif
	uchar * writer;
	if(indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), (index.size() + 2) * 4);
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize((index.size() + 2) * 4);
		writer = indexBuffer->get_address() + size;
	}
	writer = writeData(writer, usedSpace);
	writer = writeData(writer, index.size());

	vector<unsigned>::iterator iter, limit;

	for(iter = index.begin(), limit = index.end(); iter != limit; iter++) {
		writer = writeData(writer, *iter);
	}

	return OK;
}

OneConstantStatisticsBuffer* OneConstantStatisticsBuffer::load(StatisticsType type,const string path, uchar*& indexBuffer)
{
	OneConstantStatisticsBuffer* statBuffer = new OneConstantStatisticsBuffer(path, type);

	unsigned size, first;
	indexBuffer = readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = readData(indexBuffer, size);

	statBuffer->index.resize(0);

	statBuffer->indexSize = size;

	for( unsigned i = 0; i < size; i++ ) {
		indexBuffer = readData(indexBuffer, first);
		statBuffer->index.push_back(first);
	}

	return statBuffer;
}

Status OneConstantStatisticsBuffer::getIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID)
{
	unsigned i = 0, endEntry = 0;
	unsigned begin = index[ minID / HASH_RANGE], end = 0;
	reader = (uchar*)buffer->getBuffer() + begin;

	i = minID / ID_HASH;
	while(i < indexSize) {
		if(index[i] != 0) {
			end = index[i];
			break;
		}
		i++;
	}
	if(i == indexSize)
		end = usedSpace;
	endEntry = i;

	const uchar* limit = (uchar*)buffer->getBuffer() + end;

	readData(reader, lastId);
	decode(reader, limit);
	if ( lastId != minID ) {
		find(minID);
	}

	i = maxID / ID_HASH + 1;
	unsigned end1;
	while(index[i] == 0 && i < indexSize) {
		i++;
	}
	if(i == indexSize)
		end1 = usedSpace;
	else
		end1 = index[i];

	while(true) {
		if(end == end1) {
			Triple* temp = pos;
			if(find(maxID) == true)
				posLimit = pos + 1;
			pos = temp;
		}

		while(pos < posLimit) {
			entBuffer->insertID(pos->value1);
			pos++;
		}

		begin = end;
		if(begin == end1)
			break;

		endEntry = endEntry + 1;
		while(index[endEntry] != 0 && endEntry < indexSize) {
			endEntry++;
		}
		if(endEntry == indexSize) {
			end = usedSpace;
		} else {
			end = index[endEntry];
		}

		reader = (const unsigned char*)buffer->getBuffer() + begin;
		limit = (const unsigned char*)buffer->getBuffer() + end;
		decode(reader, limit);
	}

	return OK;
}

//////////////////////////////////////////////////////////////////////

TwoConstantStatisticsBuffer::TwoConstantStatisticsBuffer(const string path, StatisticsType type) : StatisticsBuffer(), type(type), reader(NULL)
{
	buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	//index = (Triple*)malloc(MemoryBuffer::pagesize * sizeof(Triple));
	writer = (uchar*)buffer->getBuffer();
	lastId = 0; lastPredicate = 0;
	usedSpace = 0;
	indexPos = 0;
	indexSize = 0; //MemoryBuffer::pagesize;
	index = NULL;

	first = true;
}

TwoConstantStatisticsBuffer::~TwoConstantStatisticsBuffer()
{
	if(buffer != NULL) {
		delete buffer;
	}
	buffer = NULL;
	index = NULL;
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end)
{
	unsigned value1, value2, count;
	Triple* writer = &triples[0];
	readData(begin, value1);
	while (begin < end && value1) {
		begin = readData(begin, value1);
		begin = readData(begin, value2);
		begin = readData(begin, count);
		(*writer).value1 = value1;
		(*writer).value2 = value2;
		(*writer).count = count;
		++writer;
	}

	// Update the entries
	pos=triples;
	posLimit=writer;

	return begin;
}

static inline bool greater(unsigned a1,unsigned a2,unsigned b1,unsigned b2) {
   return (a1>b1)||((a1==b1)&&(a2>b2));
}

static inline bool less(unsigned a1, unsigned a2, unsigned b1, unsigned b2) {
	return (a1 < b1) || ((a1 == b1) && (a2 < b2));
}
/*
 * find the first entry >= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find(unsigned value1, unsigned value2)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);
		if (::greater(value1, value2, pos[middle].value1, pos[middle].value2)) {
			left = middle + 1;
		} else if ((!middle) || ::greater(value1, value2, pos[middle - 1].value1, pos[middle -1].value2)) {
			break;
		} else {
			right = middle;
		}
	}

	if(left == right) {
		return false;
	} else {
#ifdef MYDEBUG
	cout << "middle: " << middle << endl;
#endif
		pos = &pos[middle];// value1 and value2 is between middle-1 and middle
		return true;
	}
}

/*
 * find the last entry <= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find_last(unsigned value1, unsigned value2)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle = 0;

	while (left < right) {
		middle = left + ((right - left) / 2);
		if (::less(value1, value2, pos[middle].value1, pos[middle].value2)) {
			right = middle;
		} else if ((!middle) || ::less(value1, value2, pos[middle + 1].value1, pos[middle + 1].value2)) {
			break;
		} else {
			left = middle + 1;
		}
	}

	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

int TwoConstantStatisticsBuffer::findPredicate(unsigned value1,Triple*pos,Triple* posLimit){
	int low = 0, high= posLimit - pos,mid;
	while (low <= high) { //当前查找区间R[low..high]非空
		mid = low + ((high - low)/2);
		if (pos[mid].value1 == value1)
			return mid; //查找成功返回
		if (pos[mid].value1 > value1)
			high = mid - 1; //继续在R[low..mid-1]中查扄1�7
		else
			low = mid + 1; //继续在R[mid+1..high]中查扄1�7
	}
	return -1; //当low>high时表示查找区间为空，查找失败

}

Status TwoConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2)
{
#ifdef MYDEBUG
	cout << v1 << "\t" << v2 << endl;
#endif
	pos = index, posLimit = index + indexPos;
	find(v1, v2); // get index location, that is pos
	if(::greater(pos->value1, pos->value2, v1, v2))
		pos--;

	unsigned start = pos->count; pos++;
	unsigned end = pos->count; // count is usedspace
	if(pos == (index + indexPos))
		end = usedSpace;
#ifdef MYDEBUG
	cout << "usedSpace: " << usedSpace << endl;
#endif
	const unsigned char* begin = (uchar*)buffer->getBuffer() + start, *limit = (uchar*)buffer->getBuffer() + end;
	decode(begin, limit);//decode from bitmapbuffer, in order to get pos and posLimit
	find(v1, v2);
	if(pos->value1 == v1 && pos->value2 == v2) {
		v1 = pos->count;
		return OK;
	}
	v1 = 0;
	return NOT_FOUND;
}

Status TwoConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3)
{
/*
#ifdef MYDEBUG
	cout << "OneConstantStatisticsBuffer: " << v1 << "\t" << v2 << "\t" << v3 << endl;
#endif
*/

	unsigned len = 4 * 3;

	if (first || usedSpace + len > buffer->getSize()) {
		usedSpace = writer - (uchar*) buffer->getBuffer();
		buffer->resize(
				STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);	//加大空间
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

		index[indexPos].value1 = v1;
		index[indexPos].value2 = v2;
		index[indexPos].count = usedSpace; //record offset，可以得出实体——谓词所在的块号

		indexPos++;
		first = false;
	}

	writer = writeData(writer, v1);
	writer = writeData(writer, v2);
	writer = writeData(writer, v3);

	lastId = v1;
	lastPredicate = v2;
	usedSpace = writer - (uchar*) buffer->getBuffer();

	return OK;
}

Status TwoConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer)
{
	uchar* writer;
	if(indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize(indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address() + size;
	}

	writer = writeData(writer, usedSpace);
	writer = writeData(writer, indexPos);

	memcpy(writer, (char*)index, indexPos * sizeof(Triple));
#ifdef MYDEBUG
	for(int i = 0; i < indexPos; i++)
	{
		cout<<index[i].value1<<" : "<<index[i].value2<<" : "<<index[i].count<<endl; // count is usedspace
	}

	cout<<"indexPos: "<<indexPos<<endl;
#endif
	free(index);

	return OK;
}

TwoConstantStatisticsBuffer* TwoConstantStatisticsBuffer::load(StatisticsType type, const string path, uchar*& indexBuffer)
{
	TwoConstantStatisticsBuffer* statBuffer = new TwoConstantStatisticsBuffer(path, type);

	indexBuffer = readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = readData(indexBuffer, statBuffer->indexPos);
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	statBuffer->index = (Triple*)indexBuffer;
	indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);

#ifdef DEBUG
	for(int i = 0; i < 3; i++)
	{
		cout<<statBuffer->index[i].value1<<" : "<<statBuffer->index[i].value2<<" : "<<statBuffer->index[i].count<<endl;
	}
#endif

	return statBuffer;
}

Status TwoConstantStatisticsBuffer::getPredicatesByID(unsigned id,EntityIDBuffer* entBuffer, ID minID, ID maxID) {
	Triple* pos, *posLimit;
	pos = index;
	posLimit = index + indexPos;
	find(id, pos, posLimit);
	//cout << "findchunk:" << pos->value1 << "  " << pos->value2 << endl;
	assert(pos >= index && pos < posLimit);
	Triple* startChunk = pos;
	Triple* endChunk = pos;
	while (startChunk->value1 > id && startChunk >= index) {
		startChunk--;
	}
	while (endChunk->value1 <= id && endChunk < posLimit) {
		endChunk++;
	}

	const unsigned char* begin, *limit;
	Triple* chunkIter = startChunk;

	while (chunkIter < endChunk) {
		//		cout << "------------------------------------------------" << endl;
		begin = (uchar*) buffer->get_address() + chunkIter->count;
		//		printf("1: %x  %x  %u\n",begin, buffer->get_address() ,chunkIter->count);
		chunkIter++;
		if (chunkIter == index + indexPos)
			limit = (uchar*) buffer->get_address() + usedSpace;
		else
			limit = (uchar*) buffer->get_address() + chunkIter->count;
		//		printf("2: %x  %x  %u\n",limit, buffer->get_address() ,chunkIter->count);

		Triple* triples = new Triple[3 * MemoryBuffer::pagesize];
		decode(begin, limit, triples, pos, posLimit);

		int mid = findPredicate(id, pos, posLimit), loc = mid;
		//		cout << mid << "  " << loc << endl;


		if (loc == -1)
			continue;
		entBuffer->insertID(pos[loc].value2);
		//	cout << "result:" << pos[loc].value2<< endl;
		while (pos[--loc].value1 == id && loc >= 0) {
			entBuffer->insertID(pos[loc].value2);
			//			cout << "result:" << pos[loc].value2<< endl;
		}
		loc = mid;
		while (pos[++loc].value1 == id && loc < posLimit - pos) {
			entBuffer->insertID(pos[loc].value2);
			//			cout << "result:" << pos[loc].value2<< endl;
		}
		delete triples;
	}

	//	entBuffer->print();
	return OK;
}

bool TwoConstantStatisticsBuffer::find(unsigned value1,Triple*& pos,Triple*& posLimit)
{//find by the value1
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
//	cout << "right:" << right << endl;
	int middle=0;

	while (left < right) {
		middle = left + ((right - left) / 2);
//		cout << "first:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
		if (value1 > pos[middle].value1) {
			left = middle +1;
		} else if ((!middle) || value1 > pos[middle - 1].value1) {
//			cout << "break1:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
			break;
		} else {
			right = middle;
		}
	}

	if(left == right) {
		pos = &pos[middle];
		return false;
	} else {
		pos = &pos[middle];
//		cout << "pos[middle]:" << pos[middle].value1 << "  " << pos[middle].value2 << endl;
		return true;
	}
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end,Triple*triples,Triple* &pos,Triple* &posLimit)
{
	unsigned value1, value2, count;
	Triple* writer = &triples[0];
	readData(begin, value1);
	while (begin < end && value1) {
		begin = readData(begin, value1);
		begin = readData(begin, value2);
		begin = readData(begin, count);
		(*writer).value1 = value1;
		(*writer).value2 = value2;
		(*writer).count = count;
		++writer;
	}

	// Update the entries
	pos = triples;
	posLimit = writer;

	return begin;
}
