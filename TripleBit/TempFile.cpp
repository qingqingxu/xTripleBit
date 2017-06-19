#include "TempFile.h"
#include <sstream>
#include <cassert>
#include <cstring>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// The next id
unsigned TempFile::id = 0;
//---------------------------------------------------------------------------
string TempFile::newSuffix()
// Construct a new suffix
{
	stringstream buffer;
	buffer << '.' << (id++);
	return buffer.str();
}
//---------------------------------------------------------------------------
TempFile::TempFile(const string& baseName) :
		baseName(baseName), fileName(baseName + newSuffix()), out(
				fileName.c_str(), ios::out | ios::binary | ios::trunc), writePointer(
				0)
// Constructor
{
}
//---------------------------------------------------------------------------
TempFile::~TempFile()
// Destructor
{
//	discard();
}
//---------------------------------------------------------------------------
void TempFile::flush()
// Flush the file
{
	if (writePointer) {
		out.write(writeBuffer, writePointer);
		writePointer = 0;
	}
	out.flush();
}
//---------------------------------------------------------------------------
void TempFile::close()
// Close the file
{
	flush();
	out.close();
}
//---------------------------------------------------------------------------
void TempFile::discard()
// Discard the file
{
	close();
	remove(fileName.c_str());
}

void TempFile::writeID(ID id) {
	if (writePointer + sizeof(ID) > bufferSize) {
		out.write(writeBuffer, writePointer);
		writePointer = 0;
	}
	*((ID*)(writeBuffer + writePointer)) = ID;
	writePointer += sizeof(ID);
}

void TempFile::write(double data, char dataType) {
	if (writePointer + sizeof(double) + sizeof(char) > bufferSize) {
		out.write(writeBuffer, writePointer);
		writePointer = 0;
	}
	*(double*) (writeBuffer + writePointer) = data;
	writePointer += sizeof(double);
	*(char*) (writeBuffer + writePointer) = dataType;
	writePointer += sizeof(char);
}

void TempFile::writeTriple(ID subjectID, ID predicateID, double object,
		char objType) {
	if (writePointer + sizeof(ID) > bufferSize) {//s
		out.write(writeBuffer, writePointer);
		writePointer = 0;
	}
	*(ID*) (writeBuffer + writePointer) = subjectID;
	writePointer += sizeof(ID);

	if (writePointer + sizeof(ID) > bufferSize) {//p
		out.write(writeBuffer, writePointer);
		writePointer = 0;
	}
	*(ID*) (writeBuffer + writePointer) = predicateID;
	writePointer += sizeof(ID);

	write(object, objType);//o
}

const uchar* TempFile::readID(const uchar* reader, ID& data) {
	data = *(ID*)reader;
	reader += sizeof(ID);
	return reader;
}

const uchar* TempFile::read(const uchar* reader, double& data,
		char& dataType) {
	data = *(double*)reader;
	reader += sizeof(double);
	dataType = *(char*)reader;
	reader += sizeof(char);
	return reader;
}

const uchar* TempFile::readTriple(const uchar* reader, ID& subjectID,
		ID& predicateID, double& object, char& objType) {
	reader = readID(readID(reader, subjectID), predicateID);
	return read(reader, object, objType);
}

uint TempFile::getLen(char dataType) {
	int len;
	switch (dataType) {
	case BOOL:
	case CHAR:
		len = sizeof(char);
		break;
	case INT:
	case FLOAT:
		len = sizeof(float);
		break;
	case DATE:
	case DOUBLE:
	case LONGLONG:
		len = sizeof(double);
		break;
	case UNSIGNED_INT:
	case STRING:
	default:
		len = sizeof(uint);
		break;
	}
	return len;
}

const uchar* TempFile::skipId(const uchar* reader) {
	return reader + sizeof(ID);
}

const uchar* TempFile::skipObject(const uchar* reader){
	return reader + sizeof(double) + sizeof(char);
}

//---------------------------------------------------------------------------
void TempFile::write(unsigned len, const uchar* data)
// Raw write
		{
	// Fill the buffer
	if (writePointer + len > bufferSize) {
		unsigned remaining = bufferSize - writePointer;
		memcpy(writeBuffer + writePointer, data, remaining);
		out.write(writeBuffer, bufferSize);
		writePointer = 0;
		len -= remaining;
		data += remaining;
	}
	// Write big chunks if any
	if (writePointer + len > bufferSize) {
		assert(writePointer == 0);
		unsigned chunks = len / bufferSize;
		out.write((const char*)data, chunks * bufferSize);
		len -= chunks * bufferSize;
		data += chunks * bufferSize;
	}
	// And fill the rest
	memcpy(writeBuffer + writePointer, data, len);
	writePointer += len;
}
//---------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////////////

#if defined(WIN32)||defined(__WIN32__)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
#ifdef CONFIG_WINDOWS
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include <stdio.h>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//----------------------------------------------------------------------------
// OS dependent data
struct MemoryMappedFile::Data {
#ifdef CONFIG_WINDOWS
	/// The file
	HANDLE file;
	/// The mapping
	HANDLE mapping;
#else
	/// The file
	int file;
	/// The mapping
	void* mapping;
#endif
};
//----------------------------------------------------------------------------
MemoryMappedFile::MemoryMappedFile() :
		data(0), begin(0), end(0)
// Constructor
{
}
//----------------------------------------------------------------------------
MemoryMappedFile::~MemoryMappedFile()
// Destructor
{
	close();
}
//----------------------------------------------------------------------------
bool MemoryMappedFile::open(const char* name)
// Open
		{
	if (!name)
		return false;
	close();

#ifdef CONFIG_WINDOWS
	HANDLE file = CreateFile(name, GENERIC_READ, FILE_SHARE_READ, 0,
	OPEN_EXISTING, 0, 0);
	if (file == INVALID_HANDLE_VALUE)
		return false;
	DWORD size = GetFileSize(file, 0);
	HANDLE mapping = CreateFileMapping(file, 0, PAGE_READONLY, 0, size, 0);
	if (mapping == INVALID_HANDLE_VALUE) {
		CloseHandle(file);
		return false;
	}
	begin =
			static_cast<char*>(MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, size));
	if (!begin) {
		CloseHandle(mapping);
		CloseHandle(file);
		return false;
	}
	end = begin + size;
#else
	int file=::open(name,O_RDONLY);
	if (file<0) return false;
	size_t size=lseek(file,0,SEEK_END);
	if (!(~size)) {::close(file); return false;}
	void* mapping=mmap(0,size,PROT_READ,MAP_PRIVATE,file,0);
	if (mapping == MAP_FAILED) {
		::close(file);
		return false;
	}
	begin=static_cast<uchar*>(mapping);
	end=begin+size;
#endif
	data = new Data();
	data->file = file;
	data->mapping = mapping;

	return true;
}
//----------------------------------------------------------------------------
void MemoryMappedFile::close()
// Close
{
	if (data) {
#ifdef CONFIG_WINDOWS
		UnmapViewOfFile(const_cast<uchar*>(begin));
		CloseHandle(data->mapping);
		CloseHandle(data->file);
#else
		munmap(data->mapping,end-begin);
		::close(data->file);
#endif
		delete data;
		data = 0;
		begin = end = 0;
	}
}
unsigned sumOfItAll;
//----------------------------------------------------------------------------
void MemoryMappedFile::prefetch(const uchar* start, const uchar* end)
// Ask the operating system to prefetch a part of the file
		{
	if ((end < start) || (!data))
		return;

#ifdef CONFIG_WINDOWS
	// XXX todo
#else
	posix_fadvise(data->file,start-begin,end-start+1,POSIX_FADV_WILLNEED);
#endif
}
//----------------------------------------------------------------------------
