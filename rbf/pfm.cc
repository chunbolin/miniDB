#include <unistd.h>
#include "pfm.h"

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


bool fileExist(const std::string &fileName) {
    return access(fileName.c_str(), 0) == 0;
}

RC PagedFileManager::createFile(const std::string &fileName) {
    //create data file
    if (fileExist(fileName)) return FILE_EXISTS;
    fstream fs;
    fs.open(fileName, fstream::out);
    if (fs.fail()) return FILE_CREATE_FAILED;
    char *hiddenPage = (char *) malloc(PAGE_SIZE);
    unsigned initCounter = 0;
    memcpy(hiddenPage, &initCounter, sizeof(unsigned)); //readPageCounter
    memcpy(hiddenPage + sizeof(unsigned), &initCounter, sizeof(unsigned)); //writePageCounter
    memcpy(hiddenPage + 2 * sizeof(unsigned), &initCounter, sizeof(unsigned)); //appendPageCounter
    fs.write(hiddenPage, PAGE_SIZE);
    fs.close();

    //create freeList file
    string freeListFileName = fileName + freeListFileNameSuffix;
    if (fileExist(freeListFileName)) return FILE_EXISTS;
    fs.open(freeListFileName, fstream::out);
    if (fs.fail()) return FILE_CREATE_FAILED;
    fs.close();

    free(hiddenPage);
    return OK;
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (!fileExist(fileName)) return FILE_NOT_FOUND;
    remove(fileName.c_str());
    string freeListFileName = fileName + freeListFileNameSuffix;
    if (!fileExist(freeListFileName)) return FILE_NOT_FOUND;
    remove(freeListFileName.c_str());
    return OK;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return fileHandle.openFile(fileName);
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    return fileHandle.closeFile();
}

FileHandle::FileHandle() {
    this->readPageCounter = 0;
    this->writePageCounter = 0;
    this->appendPageCounter = 0;

    this->freeListData = nullptr;
    this->freeListCapacity = 0;
}

FileHandle::~FileHandle() {
    this->closeFile();
    if (freeListData) {
        free(freeListData);
        freeListData = nullptr;
    }
}

RC FileHandle::openFile(const std::string &fileName) {
    if (!fileExist(fileName)) return FILE_NOT_FOUND;
    if (this->dataFs.is_open()) return FILE_HANDLE_OCCUPIED;
    this->dataFs.open(fileName);
    this->readCounterValues();

    //init freeList
    this->initFreeList();

    return OK;
}

RC FileHandle::closeFile() {
    //store freeList
    this->storeFreeList();

    this->freeListFs.close();
    if (!this->dataFs.is_open()) return FILE_NOT_FOUND;
    this->updateCounterValues();
    this->dataFs.close();

    return OK;
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum >= this->getNumberOfPages()) return PAGE_NUMBER_EXCEEDS;
    this->dataFs.seekg((pageNum + 1) * PAGE_SIZE);
    this->dataFs.read((char *) data, PAGE_SIZE);
    this->readPageCounter++;
    this->updateCounterValues();
    return OK;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum >= this->getNumberOfPages()) return PAGE_NUMBER_EXCEEDS;
    this->dataFs.seekp((pageNum + 1) * PAGE_SIZE);
    this->dataFs.write((char *) data, PAGE_SIZE);
    this->writePageCounter++;
    this->updateCounterValues();
    return OK;
}

RC FileHandle::appendPage(const void *data) {
    this->dataFs.seekp(0, fstream::end);
    this->dataFs.write((char *) data, PAGE_SIZE);
    this->appendPageCounter++;
    this->updateCounterValues();

    this->extendFreeList();

    return OK;
}

unsigned FileHandle::getNumberOfPages() {
    this->dataFs.seekg(0, fstream::end);
    return this->dataFs.tellg() / PAGE_SIZE - 1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return OK;
}

RC FileHandle::readCounterValues() {
    this->dataFs.seekg(0);

    char *hiddenPage = (char *) malloc(PAGE_SIZE);
    this->dataFs.read(hiddenPage, PAGE_SIZE);
    memcpy(&this->readPageCounter, hiddenPage, sizeof(unsigned)); //readPageCounter
    memcpy(&this->writePageCounter, hiddenPage + sizeof(unsigned), sizeof(unsigned)); //writePageCounter
    memcpy(&this->appendPageCounter, hiddenPage + 2 * sizeof(unsigned), sizeof(unsigned)); //appendPageCounter

    free(hiddenPage);
    return OK;
}

RC FileHandle::updateCounterValues() {
    this->dataFs.seekg(0);
    char *hiddenPage = (char *) malloc(PAGE_SIZE);
    memcpy(hiddenPage, &this->readPageCounter, sizeof(unsigned)); //readPageCounter
    memcpy(hiddenPage + sizeof(unsigned), &this->writePageCounter, sizeof(unsigned)); //writePageCounter
    memcpy(hiddenPage + 2 * sizeof(unsigned), &this->appendPageCounter, sizeof(unsigned)); //appendPageCounter
    dataFs.write(hiddenPage, PAGE_SIZE);
    free(hiddenPage);
    return OK;
}

RC FileHandle::initFreeList() {
    //init freeList
    unsigned pageCounter = this->getNumberOfPages();
    if (pageCounter != 0) {
        freeListData = (char *) malloc(sizeof(short) * pageCounter);
        freeListCapacity = pageCounter;
        this->freeListFs.seekg(0);
        this->freeListFs.read(freeListData, sizeof(short) * pageCounter);
    }
    return OK;
}

RC FileHandle::extendFreeList() {
    //extend freeList
    if (freeListCapacity == 0) {
        freeListCapacity = 8; //init freeList capacity
        freeListData = (char *) malloc(sizeof(short) * freeListCapacity);
    } else if (this->getNumberOfPages() > freeListCapacity) {
        auto *newFreeList = (char *) malloc(sizeof(short) * freeListCapacity * 2);
        memcpy(newFreeList, freeListData, sizeof(short) * freeListCapacity);
        free(freeListData);
        freeListData = newFreeList;
        freeListCapacity *= 2;
    }
    return OK;
}

RC FileHandle::storeFreeList() {
    //store freeList
    unsigned pageCounter = this->getNumberOfPages();
    if (pageCounter != 0) {
        this->freeListFs.seekg(0);
        this->freeListFs.write(freeListData, sizeof(short) * pageCounter);
        free(freeListData);
    }
    return OK;
}

