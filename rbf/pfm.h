#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;

#define PAGE_SIZE 4096

#include <string>
#include <fstream>
#include <cstring>

using namespace std;

enum ReturnCode {
    OK = 0,

    FILE_EXISTS = 1,
    FILE_CREATE_FAILED = 2,
    FILE_NOT_FOUND = 3,
    FILE_HANDLE_OCCUPIED = 4,
    FILE_HANDLE_NOT_FOUND = 4,
    FILE_REMOVE_FAILED = 5,
    FILE_SEEK_ERR = 6,

    HEAD_MSG_READ_ERR = 7,
    HEAD_MSG_WRITE_ERR = 8,
    FREE_LIST_READ_ERR = 9,
    FREE_LIST_WRITE_ERR = 10,


    PAGE_READ_ERR = 11,
    PAGE_WRITE_ERR = 12,
    PAGE_NUMBER_EXCEEDS = 13,
    LOAD_FILE_FAILED = 14,
    CLOSE_FILE_FAILED = 15,

    RECORD_TOO_LARGE = 16,
    RECORD_READ_ERR = 17
};

const string freeListFileNameSuffix = "&freeList";

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

private:
    static PagedFileManager *_pf_manager;

};

class FileHandle {
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    char *freeListData;
    unsigned freeListCapacity;

    FileHandle();                                                       // Default constructor
    ~FileHandle();                                                      // Destructor

    RC openFile(const std::string &fileName);

    RC closeFile();

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                            unsigned &appendPageCount);                 // Put current counter values into variables

    RC readCounterValues();

    RC updateCounterValues();

    RC initFreeList();

    RC extendFreeList();

    RC storeFreeList();

private:
    fstream dataFs;
    fstream freeListFs;
};

#endif