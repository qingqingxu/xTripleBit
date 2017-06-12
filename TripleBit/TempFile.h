#ifndef _TEMPFILE_H_
#define _TEMPFILE_H_

#include "TripleBit.h"
#include <fstream>
#include <string>
//---------------------------------------------------------------------------
#if defined(_MSC_VER)
typedef unsigned __int64 uint64_t;
#else
#include <stdint.h>
#endif
//---------------------------------------------------------------------------
/// A temporary file
class TempFile {
private:
	/// The next id
	static unsigned id;

	/// The base file name
	std::string baseName;
	/// The file name
	std::string fileName;
	/// The output
	std::ofstream out;

	/// The buffer size
	static const unsigned bufferSize = 16384;
	/// The write buffer
	char writeBuffer[bufferSize];
	/// The write pointer
	unsigned writePointer;

	/// Construct a new suffix
	static std::string newSuffix();

public:
	/// Constructor
	TempFile(const std::string& baseName);
	/// Destructor
	~TempFile();

	/// Get the base file name
	const std::string& getBaseFile() const {
		return baseName;
	}
	/// Get the file name
	const std::string& getFile() const {
		return fileName;
	}

	/// Flush the file
	void flush();
	/// Close the file
	void close();
	/// Discard the file
	void discard();

	void writeId(ID id);
	/// Raw write
	void write(unsigned len, const char* data);//n
	template<typename T>
	void write(const uchar* writer, T& data, DataType dataType = DataType::STRING);
	template<typename T>
	void writeTriple(const uchar* writer, T& subject, T& predicate, T& object, DataType objType = DataType::STRING);
	/// Skip a predicate
	static const char* skipId(const char* reader);//n
	static const uchar* shipTriple(const uchar* reader);
	/// Read an id
	static const char* readId(const char* reader, ID& id);//n
	template<typename T>
	static const uchar* read(const uchar* reader, T& data, DataType dataType = DataType::STRING);
	template<typename T>
	static const uchar* readTriple(const uchar* reader, T& subject, T& predicate, T& object);
};

//----------------------------------------------------------------------------
/// Maps a file read-only into memory
class MemoryMappedFile
{
   private:
   /// os dependent data
   struct Data;

   /// os dependen tdata
   Data* data;
   /// Begin of the file
   const char* begin;
   /// End of the file
   const char* end;

   public:
   /// Constructor
   MemoryMappedFile();
   /// Destructor
   ~MemoryMappedFile();

   /// Open
   bool open(const char* name);
   /// Close
   void close();

   /// Get the begin
   const char* getBegin() const { return begin; }
   /// Get the end
   const char* getEnd() const { return end; }

   /// Ask the operating system to prefetch a part of the file
   void prefetch(const char* start,const char* end);
};
//---------------------------------------------------------------------------
#endif
