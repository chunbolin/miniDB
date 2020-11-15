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

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    if (!_pf_manager) {
        _pf_manager = new PagedFileManager();
    }
    return *_pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() {
    if (_pf_manager) {
        delete _pf_manager;
        _pf_manager = nullptr;
    }
}

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

RC PagedFileManager::createFile(const std::string &fileName) {
    FILE *check = fopen(fileName.c_str(), "rb");
    if (check) {
        return rc::FILE_EXISTS;
    }
    FILE *file = fopen(fileName.c_str(), "wb");
    if (!file) {
        return rc::FILE_CREATE_FAILED;
    }
    //init all counter
    FileMsg fileMsg{0, 0, 0, 0};
    writeCounterValues(file, &fileMsg);
    fclose(file);
    return rc::OK;

}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    // Test whether filName already exists
    FILE *file = fopen(fileName.c_str(), "rb");
    if (!file) {
        return rc::FILE_NOT_FOUND;
    }
    fclose(file);
    int res = remove(fileName.c_str());
    if (res != 0) {
        return rc::FILE_REMOVE_FAILED;
    }
    return rc::OK;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    FILE *file = fopen(fileName.c_str(), "rb");
    if (!file) {
        return rc::FILE_NOT_FOUND;
    }
    fclose(file);
    file = fopen(fileName.c_str(), "rw+b");
    fileHandle.loadFile(file);
    return rc::OK;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (&fileHandle == nullptr) {
        return rc::FILE_HANDLE_NOT_FOUND;
    }
    return fileHandle.close();
}

FileHandle::FileHandle() {
    this->pageCounter = 0;
    this->readPageCounter = 0;
    this->writePageCounter = 0;
    this->appendPageCounter = 0;
    this->_file = nullptr;
    this->freeListData = nullptr;
    this->freeListCapacity = 0;
}

FileHandle::~FileHandle() {
    if (_file) {
        _file = nullptr;
    }
}

RC FileHandle::loadFile(FILE *file) {
    FileMsg fileMsg{};
    rewind(file);
    size_t readSize = fread(&fileMsg, sizeof(FileMsg), 1, file);
    if (readSize != 1) {
        return rc::HEAD_MSG_READ_ERR;
    }
    readCounterValues(file, &fileMsg);
    this->pageCounter = fileMsg.pageCounter;
    this->readPageCounter = fileMsg.readPageCounter;
    this->writePageCounter = fileMsg.writePageCounter;
    this->appendPageCounter = fileMsg.appendPageCounter;
    this->_file = file;

    //init freeList
    if (pageCounter != 0) {
        freeListData = malloc(sizeof(short) * pageCounter);
        freeListCapacity = pageCounter;
        int res = fseek(_file, sizeof(FileMsg) + pageCounter * PAGE_SIZE, SEEK_SET);
        if (res != 0) {
            return rc::FILE_SEEK_ERR;
        }
        readSize = fread(freeListData, sizeof(short) * pageCounter, 1, _file);
        if (readSize != 1) {
            return rc::FREE_LIST_READ_ERR;
        }
    }
    return rc::OK;
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum >= pageCounter) return rc::PAGE_NUMBER_EXCEEDS;
    int res = fseek(_file, sizeof(FileMsg) + pageNum * PAGE_SIZE, SEEK_SET);
    if (res != 0) {
        return rc::FILE_SEEK_ERR;
    }
    size_t readSize = fread(data, PAGE_SIZE, 1, _file);
    if (readSize != 1) {
        return rc::PAGE_READ_ERR;
    }
    readPageCounter++;
    return rc::OK;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum >= pageCounter) return -1;
    int res = fseek(_file, sizeof(FileMsg) + pageNum * PAGE_SIZE, SEEK_SET);
    if (res != 0) {
        return rc::FILE_SEEK_ERR;
    }
    size_t writeSize = fwrite(data, PAGE_SIZE, 1, _file);
    if (writeSize != 1) {
        return rc::PAGE_WRITE_ERR;
    }
    writePageCounter++;
    return rc::OK;
}

RC FileHandle::appendPage(const void *data) {
    int res = fseek(_file, sizeof(FileMsg) + pageCounter * PAGE_SIZE, SEEK_SET);
    if (res != 0) {
        return rc::FILE_SEEK_ERR;
    }
    size_t writeSize = fwrite(data, PAGE_SIZE, 1, _file);
    if (writeSize != 1) {
        return rc::PAGE_WRITE_ERR;
    }
    pageCounter++;
    appendPageCounter++;

    //extend freeList
    if (freeListCapacity == 0) {
        freeListCapacity = 8; //init freeList capacity
        freeListData = (short *) malloc(sizeof(short) * freeListCapacity);
    } else if (pageCounter > freeListCapacity) {
        auto *newFreeList = (short *) malloc(sizeof(short) * freeListCapacity * 2);
        memcpy(newFreeList, freeListData, sizeof(short) * freeListCapacity);
        free(freeListData);
        freeListData = newFreeList;
        freeListCapacity *= 2;
    }

    return rc::OK;
}

RC FileHandle::close() {
    if (!_file) {
        return rc::CLOSE_FILE_FAILED;
    }
    //store file header data
    FileMsg fileMsg{pageCounter, readPageCounter, writePageCounter, appendPageCounter};
    rewind(_file);
    size_t writeSize = fwrite(&fileMsg, sizeof(FileMsg), 1, _file);
    if (writeSize != 1) {
        return rc::HEAD_MSG_WRITE_ERR;
    }
    //store freeList
    if (pageCounter != 0){
        int res = fseek(_file, sizeof(FileMsg) + pageCounter * PAGE_SIZE, SEEK_SET);
        if (res != 0) {
            return rc::FILE_SEEK_ERR;
        }
        writeSize = fwrite(freeListData, sizeof(short) * pageCounter, 1, _file);
        if (writeSize != 1) {
            return rc::FREE_LIST_WRITE_ERR;
        }
        free(freeListData);
    }
    //close file
    int closeRes = fclose(_file);
    if (closeRes != 0) {
        return rc::CLOSE_FILE_FAILED;
    }
    return rc::OK;
}

unsigned FileHandle::getNumberOfPages() {
    return pageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return rc::OK;
}


