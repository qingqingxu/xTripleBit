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

extern uchar* writeData(uchar* writer, uint data);
extern const char* readData(const uchar* reader, uint& data);

StatisticsBuffer::StatisticsBuffer() : HEADSPACE(2) {
	// TODO Auto-generated constructor stub
	
}

StatisticsBuffer::~StatisticsBuffer() {
	// TODO Auto-generated destructor stub
}

/////////////////////////////////////////////////////////////////
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
		begin = (const uchar*)readData(begin, value1);
		begin = (const uchar*)readData(begin, value2);
		begin = (const uchar*)readData(begin, count);
		(*writer).value1 = value1;
		(*writer).value2 = value2;
		(*writer).count = count;
		++writer;
/*
#ifdef MYDEBUG
		ofstream out;
		out.open("findvalue", ios::app);
		out << value1 << "\t" << value2 << "\t" << count << endl;
		out.close();
#endif
*/
	}

/*
#ifdef MYDEBUG
		ofstream out;
		out.open("findvalue", ios::app);
		out << "####################################" << endl;
		out.close();
#endif
*/

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
bool TwoConstantStatisticsBuffer::findPriorityByValue1(unsigned value1, unsigned value2)
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
		pos = &pos[middle];// value1 and value2 is between middle-1 and middle
		return true;
	}
}


bool TwoConstantStatisticsBuffer::findPriorityByValue2(unsigned value1, unsigned value2)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);

		if (::greater(value2, value1, pos[middle].value2, pos[middle].value1)) {
			left = middle + 1;
		} else if ((!middle) || ::greater(value2, value1, pos[middle - 1].value2, pos[middle -1].value1)) {
			break;
		} else {
			right = middle;
		}
	}

	if(left == right) {
		return false;
	} else {
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
/*
#ifdef MYDEBUG
		ofstream out;
		out.open("findvalue", ios::app);
		out << "v1: " << v1 << "\tv2: " << v2 << endl;
		out.close();
		cout << "v1: " << v1 << "\tv2: " << v2 << endl;
#endif
*/

	pos = index, posLimit = index + indexPos;
	findPriorityByValue2(v1, v2); // get index location, that is pos
	if(::greater(pos->value1, pos->value2, v1, v2))
		pos--;

	//cout << "fisrt  find: " << pos->value1 << "\t" << pos->value2 << "\t" << pos->count << endl;

	unsigned start = pos->count; pos++;
	unsigned end = pos->count; // count is usedspace
	if(pos == (index + indexPos))
		end = usedSpace;

	const unsigned char* begin = (uchar*)buffer->getBuffer() + start, *limit = (uchar*)buffer->getBuffer() + end;
	decode(begin, limit);//decode from bitmapbuffer, in order to get pos and posLimit
	findPriorityByValue2(v1, v2);
	//cout << "second find: " << pos->value1 << "\t" << pos->value2 << "\t" << pos->count << endl;
/*
#ifdef MYDEBUG
	if(find(v1, v2)){
		cout << pos->value1 << "\tfind\t" << pos->value2 << endl;
	}
#endif
*/
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
	cout << "TwoConstantStatisticsBuffer: " << v1 << "\t" << v2 << "\t" << v3 << endl;
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


#ifdef MYDEBUG
	ofstream out;
	out.open("TwoConstantStatisticsBuffer", ios::app);
	out << v1 << "\t" << v2 << "\t" << v3 << endl;
	out.close();
#endif

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
	ofstream out;
	out.open("twostatistics");
	for(uint i = 0; i < indexPos; i++)
	{
		out<<index[i].value1<<" : "<<index[i].value2<<" : "<<index[i].count<<endl; // count is usedspace
	}

	out<<"indexPos: "<<indexPos<<endl;
	out << "***********************" <<endl;
	out.close();
#endif
	free(index);

	return OK;
}

TwoConstantStatisticsBuffer* TwoConstantStatisticsBuffer::load(StatisticsType type, const string path, uchar*& indexBuffer)
{
	TwoConstantStatisticsBuffer* statBuffer = new TwoConstantStatisticsBuffer(path, type);

	indexBuffer = (uchar*)readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (uchar*)readData(indexBuffer, statBuffer->indexPos);
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	statBuffer->index = (Triple*)indexBuffer;
	indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);

#ifdef MYDEBUG
	ofstream out;
	out.open("statIndex", ios::app);
	out << "###########################" << endl;
	for (uint i = 0; i < statBuffer->indexPos; i++) {
		out << statBuffer->index[i].value1 << " : "
				<< statBuffer->index[i].value2 << " : "
				<< statBuffer->index[i].count << endl;
	}
	out.close();
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
		begin = (const uchar*)readData(begin, value1);
		begin = (const uchar*)readData(begin, value2);
		begin = (const uchar*)readData(begin, count);
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
