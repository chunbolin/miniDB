
#include "rbfm.h"


RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() = default;

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance().openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance().closeFile(fileHandle);
}

void getNullInfo(const std::vector<Attribute> &recordDescriptor, const void *data, bool nullInfo[]) {
    int attrSize = recordDescriptor.size();
    int nullInfoLen = ceil(((double) attrSize) / 8);
    char nullInfos[nullInfoLen];
    memcpy(nullInfos, data, nullInfoLen);
    int index = 0, count = 0;
    for (int i = 0; i < nullInfoLen; ++i) {
        unsigned num = nullInfos[i];
        count = 0;
        while (count < 8) {
            if (num & 128u) {
                nullInfo[index] = true;
            } else {
                nullInfo[index] = false;
            }
            num <<= 1u;
            count++;
            index++;
        }
    }
}

unsigned getRecordSize(const std::vector<Attribute> &recordDescriptor, const void *data) {
    unsigned attrSize = recordDescriptor.size();
    unsigned recordSize = ceil(((double) attrSize) / 8);
    bool nullInfo[attrSize];
    getNullInfo(recordDescriptor, data, nullInfo);
    for (int i = 0; i < attrSize; ++i) {
        if (nullInfo[i]) continue;
        Attribute attr = recordDescriptor.at(i);
        if (attr.type == TypeInt || attr.type == TypeReal) {
            recordSize += 4;
        } else if (attr.type == TypeVarChar) {
            recordSize += 4 + attr.length;
        }
    }
    return recordSize;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    unsigned recordSize = getRecordSize(recordDescriptor, data);
    bool flag = false;
    for (int i = fileHandle.getNumberOfPages() - 1; i >= 0; i--) {
        void *page = malloc(PAGE_SIZE);
        fileHandle.readPage(i, page);
        PageMsg pageMsg{};
        memcpy(&pageMsg, page, sizeof(PageMsg));
        unsigned offsets[pageMsg.tupleCount + 1];
        memcpy(offsets, (char *) page + sizeof(PageMsg), sizeof(unsigned) * pageMsg.tupleCount);
        if (pageMsg.freeSpace >= recordSize) {
            unsigned offset;
            if (pageMsg.tupleCount == 0) {
                offset = PAGE_SIZE - recordSize;
                memcpy((char *) page + offset, data, recordSize);
            } else {
                offset = offsets[pageMsg.tupleCount - 1] - recordSize;
                memcpy((char *) page + offset, data, recordSize);
            }
            offsets[pageMsg.tupleCount] = offset;
            pageMsg.tupleCount++;
            pageMsg.freeSpace -= (4 + recordSize);
            memcpy(page, &pageMsg, sizeof(pageMsg));
            memcpy((char *) page + sizeof(PageMsg), offsets, sizeof(unsigned) * pageMsg.tupleCount);
            fileHandle.writePage(i, page);
            rid.pageNum = i;
            rid.slotNum = offset;
            flag = true;
            break;
        }
    }
    if (!flag) { //append a new page
        void *page = malloc(PAGE_SIZE);
        PageMsg pageMsg{PAGE_SIZE - recordSize, 1};
        unsigned offset = PAGE_SIZE - recordSize;
        memcpy((char *) page + offset, data, recordSize);
        memcpy(page, &pageMsg, sizeof(pageMsg));
        memcpy((char *) page + sizeof(PageMsg), &offset, sizeof(unsigned));
        fileHandle.appendPage(page);
        rid.pageNum = fileHandle.getNumberOfPages() - 1;
        rid.slotNum = offset;
    }
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    PageMsg pageMsg{};
    memcpy(&pageMsg, page, sizeof(PageMsg));
    unsigned offsets[pageMsg.tupleCount];
    memcpy(offsets, (char *) page + sizeof(PageMsg), sizeof(unsigned) * pageMsg.tupleCount);
    if (pageMsg.tupleCount == 0) {
        memcpy(data, (char *) page + rid.slotNum, PAGE_SIZE - rid.slotNum);
    } else {
        int i = 0;
        for (; i < pageMsg.tupleCount; ++i) {
            if (offsets[i] == rid.slotNum) {
                break;
            }
        }
        int recordSize;
        if (i == pageMsg.tupleCount - 1) {
            recordSize = PAGE_SIZE - rid.slotNum;
        } else {
            recordSize = rid.slotNum - offsets[i + 1];
        }
        memcpy(data, (char *) page + rid.slotNum, recordSize);
    }
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    unsigned attrSize = recordDescriptor.size();
    unsigned curr = ceil(((double) attrSize) / 8);
    bool nullInfo[attrSize];
    getNullInfo(recordDescriptor, data, nullInfo);
    for (int i = 0; i < attrSize; ++i) {
        Attribute attr = recordDescriptor.at(i);
        std::cout << attr.name << ": ";
        if (nullInfo[i])
            std::cout << "NULL  ";
        else {
            if (attr.type == TypeInt) {
                int num;
                memcpy(&num, (char *) data + curr, sizeof(int));
                std::cout << num << "  ";
                curr += 4;
            } else if (attr.type == TypeReal) {
                float num;
                memcpy(&num, (char *) data + curr, sizeof(float));
                std::cout << num << "  ";
                curr += 4;
            } else if (attr.type == TypeVarChar) {
                int len;
                memcpy(&len, (char *) data + curr, sizeof(int));
                curr += 4;
                char str[len + 1];
                memcpy(str, (char *) data + curr, len);
                str[len] = '\0';
                curr += len;
                std::cout << str << "  ";
            }
        }
    }

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}



