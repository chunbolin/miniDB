#include "pfm.h"
#include <cstdio>

//file header msg
struct FileMsg {
    unsigned pageCounter; //page number in current file
    unsigned readPageCounter; //total page read count
    unsigned writePageCounter; // total page write count
    unsigned appendPageCounter; // total page append count
};

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

        fileHandle = *(new FileHandle(fileMsg.pageCounter, file,
                                      fileMsg.readPageCounter, fileMsg.writePageCounter, fileMsg.appendPageCounter));
        return 0;
    } else {
        return -1;
    }

}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    fileHandle.close();
    return 0;
}

FileHandle::FileHandle() {
    pageCounter = 0;
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::FileHandle(unsigned pageCounter, FILE *file) {
    this->pageCounter = pageCounter;
    this->file = file;
    this->readPageCounter = 0;
    this->writePageCounter = 0;
    this->appendPageCounter = 0;
}

FileHandle::FileHandle(unsigned pageCounter, FILE *file,
                       unsigned readPageCounter,
                       unsigned writePageCounter,
                       unsigned appendPageCounter) {
    this->pageCounter = pageCounter;
    this->file = file;
    this->readPageCounter = readPageCounter;
    this->writePageCounter = writePageCounter;
    this->appendPageCounter = appendPageCounter;
}


FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum >= pageCounter) return -1;
    size_t result;
    fseek(file, sizeof(FileMsg) + pageNum * PAGE_SIZE, SEEK_SET);
    result = fread(data, PAGE_SIZE, 1, this->file);
    if (result != 1) {
        fclose(file);
        return -1;
    }
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum >= pageCounter) return -1;
    fseek(file, sizeof(FileMsg) + pageNum * PAGE_SIZE, SEEK_SET);
    fwrite(data, PAGE_SIZE, 1, file);
    fflush(file);
    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    fseek(file, sizeof(FileMsg) + pageCounter * PAGE_SIZE, SEEK_SET);
    fwrite(data, PAGE_SIZE, 1, file);
    fflush(file);
    pageCounter++;
    appendPageCounter++;
    return 0;
}

RC FileHandle::close() {
    FileMsg fileMsg{pageCounter, readPageCounter, writePageCounter, appendPageCounter};
    writeCounterValues(file, &fileMsg);

    fflush(file);
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

