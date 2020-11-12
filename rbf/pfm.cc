#include "pfm.h"
#include <cstdio>

using namespace std;


RC writeCounterValues(FILE *file, FileMsg *fileMsg) {
    rewind(file);
    size_t result;
    result = fwrite(fileMsg, sizeof(FileMsg), 1, file);
    if (result != 1) {
        fclose(file);
        return -1;
    }
    return 0;
}

RC readCounterValues(FILE *file, FileMsg *fileMsg) {
    rewind(file);
    size_t result;
    result = fread(fileMsg, sizeof(FileMsg), 1, file);
    if (result != 1) {
        fclose(file);
        return -1;
    }
    return 0;
}

void handleError(size_t result, size_t supposeValue) {

}

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() = default;

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

RC PagedFileManager::createFile(const std::string &fileName) {
    FILE *check = fopen(fileName.c_str(), "rb");
    if (check == nullptr) {
        FILE *file = fopen(fileName.c_str(), "wb");

        //init all counter
        FileMsg fileMsg{0, 0, 0, 0};
        writeCounterValues(file, &fileMsg);

        fclose(file);
        return 0;
    } else {
        fclose(check);
        return -1;
    }
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (remove(fileName.c_str()) != 0) {
        perror("Error deleting file");
        return -1;
    } else {
        return 0;
    }
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    FILE *check = fopen(fileName.c_str(), "rb");
    if (check != nullptr) {
        fclose(check);
        FILE *file = fopen(fileName.c_str(), "rw+b");

        FileMsg fileMsg{};
        readCounterValues(file, &fileMsg);

        //init counters and freeList
        fileHandle.initHandle(fileMsg.pageCounter, file, fileMsg.readPageCounter, fileMsg.writePageCounter,
                              fileMsg.appendPageCounter);

        unsigned currentPageNum = fileHandle.getNumberOfPages();
        unsigned freeListCapacity = fileHandle.freeListCapacity;
        if (currentPageNum > freeListCapacity) {
            auto *newFreeList = (short *) malloc(sizeof(short) * currentPageNum);
            free(fileHandle.freeListData);
            fileHandle.freeListData = newFreeList;
            fileHandle.freeListCapacity = currentPageNum;
        }
        fileHandle.readFreeList(fileHandle.freeListData);

        return 0;
    } else {
        return -1;
    }

}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    fileHandle.writeFreeList(fileHandle.freeListData);
    free(fileHandle.freeListData);
    fileHandle.close();

    return 0;
}

FileHandle::FileHandle() = default;

FileHandle::~FileHandle() = default;

RC FileHandle::initHandle(unsigned pageCounter, FILE *file,
                          unsigned readPageCounter,
                          unsigned writePageCounter,
                          unsigned appendPageCounter) {
    this->pageCounter = pageCounter;
    this->file = file;
    this->readPageCounter = readPageCounter;
    this->writePageCounter = writePageCounter;
    this->appendPageCounter = appendPageCounter;

    freeListData = malloc(sizeof(short) * 8);
    freeListCapacity = 8;
    return 0;
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum >= pageCounter) return -1;
    fseek(file, sizeof(FileMsg) + pageNum * PAGE_SIZE, SEEK_SET);
    size_t result = fread(data, PAGE_SIZE, 1, this->file);
    if (result != 1) {
        cout << "Error read page: " << pageNum << endl;
        fclose(file);
        return -1;
    }
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum >= pageCounter) return -1;
    fseek(file, sizeof(FileMsg) + pageNum * PAGE_SIZE, SEEK_SET);
    size_t result = fwrite(data, PAGE_SIZE, 1, file);
    if (result != 1) {
        cout << "Error write page: " << pageNum << endl;
        fclose(file);
        return -1;
    }
    fflush(file);
    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    fseek(file, sizeof(FileMsg) + pageCounter * PAGE_SIZE, SEEK_SET);
    size_t result = fwrite(data, PAGE_SIZE, 1, file);
    if (result != 1) {
        cout << "Error append page" << endl;
        fclose(file);
        return -1;
    }
    fflush(file);
    pageCounter++;
    appendPageCounter++;
    return 0;
}

RC FileHandle::close() {
    FileMsg fileMsg{pageCounter, readPageCounter, writePageCounter, appendPageCounter};
    writeCounterValues(file, &fileMsg);
    fclose(file);
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    return pageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}

RC FileHandle::readFreeList(void *freeList) {
    if (pageCounter == 0) return 0; //we do not need to read freeList since its empty
    fseek(file, sizeof(FileMsg) + pageCounter * PAGE_SIZE, SEEK_SET);
    size_t result;
    result = fread(freeList, sizeof(short) * pageCounter, 1, file);
    if (result != 1) {
        cout << "Error read freeList" << endl;
        fclose(file);
        return -1;
    }
    return 0;
}

RC FileHandle::writeFreeList(void *freeList) {
    if (pageCounter == 0) return 0; //we do not need to write freeList
    fseek(file, sizeof(FileMsg) + pageCounter * PAGE_SIZE, SEEK_SET);
    size_t result;
    result = fwrite(freeList, sizeof(short) * pageCounter, 1, file);
    if (result != 1) {
        cout << "Error write freeList" << endl;
        fclose(file);
        return -1;
    }
    return 0;
}


