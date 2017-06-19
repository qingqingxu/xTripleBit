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
	static uint id;

	/// The base file name
	std::string baseName;
	/// The file name
	std::string fileName;
	/// The output
	std::ofstream out;

	/// The buffer size
	static const uint bufferSize = 16384;
	/// The write buffer
	char writeBuffer[bufferSize];
	/// The write pointer
	uint writePointer;

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

	void writeID(ID id);
	void write(double data, char dataType = STRING);
	void writeTriple(ID subjectID, ID predicateID, double object, char objType = STRING);
	void write(unsigned len, const uchar* data);
	static const uchar* readID(const uchar* reader, ID& data);
	static const uchar* read(const uchar* reader, double& data, char& dataType);
	static const uchar* readTriple(const uchar* reader, ID& subjectID, ID& predicateID, double& object, char& objType);
	static uint getLen(char dataType = STRING);
	static const uchar* skipId(const uchar* reader);
	static const uchar* skipObject(const uchar* reader);
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
   const uchar* begin;
   /// End of the file
   const uchar* end;

   public:
   /// Constructor
   MemoryMappedFile();
   /// Destructor
   ~MemoryMappedFile();

   /// Open
   bool open(const uchar* name);
   /// Close
   void close();

   /// Get the begin
   const uchar* getBegin() const { return begin; }
   /// Get the end
   const uchar* getEnd() const { return end; }

   /// Ask the operating system to prefetch a part of the file
   void prefetch(const uchar* start,const uchar* end);
};
//---------------------------------------------------------------------------
#endif
