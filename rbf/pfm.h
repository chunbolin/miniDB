#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;

#define PAGE_SIZE 4096

#include <string>

//file header msg
struct FileMsg {
    unsigned pageCounter; //page number in current file
    unsigned readPageCounter; //total page read count
    unsigned writePageCounter; // total page write count
    unsigned appendPageCounter; // total page append count
};

class FileHandle;

class PagedFileManager {
public:
    static PagedFileManager &instance();                                // Access to the _pf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new file
    RC destroyFile(const std::string &fileName);                        // Destroy a file
    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
    RC closeFile(FileHandle &fileHandle);                               // Close a file

protected:
    PagedFileManager();                                                 // Prevent construction
    ~PagedFileManager();                                                // Prevent unwanted destruction
    PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
    PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment
};

class FileHandle {
public:

    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;

    FileHandle();

    FileHandle(unsigned pageCount, FILE *file);

    FileHandle(unsigned pageCount, FILE *file, unsigned readPageCounter, unsigned writePageCounter,
               unsigned appendPageCounter);          // Default constructor
    ~FileHandle();                                                      // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                            unsigned &appendPageCount);                 // Put current counter values into variables
    RC close();

private:
    unsigned pageCounter;
    FILE *file;
};

#endif