
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
    void *page = malloc(PAGE_SIZE);
    for (int i = fileHandle.getNumberOfPages() - 1; i >= 0; i--) {
        fileHandle.readPage(i, page);
        PageMsg pageMsg{};
        memcpy(&pageMsg, page, sizeof(PageMsg));

        SlotElement slots[pageMsg.slotCount + 1];
        memcpy(slots, (char *) page + sizeof(PageMsg), sizeof(SlotElement) * pageMsg.slotCount);
        int slotNum = 0;
        bool needAddSlot = false;
        for (; slotNum < pageMsg.slotCount + 1; ++slotNum) {
            if (slotNum == pageMsg.slotCount) {
                needAddSlot = true;
                break;
            }
            if (slots[slotNum].length == 0) break;
        }

        if (pageMsg.freeEnd - pageMsg.freeStart >= recordSize) {
            unsigned offset = pageMsg.freeEnd - recordSize;

            //insert tuple
            memcpy((char *) page + offset, data, recordSize);

            //maintain pageMsg
            pageMsg.tupleCount++;
            if (needAddSlot) {
                pageMsg.slotCount++;
                pageMsg.freeStart += sizeof(SlotElement);
            }
            pageMsg.freeEnd -= recordSize;
            memcpy(page, &pageMsg, sizeof(pageMsg));

            //maintain slots
            slots[slotNum].offset = offset;
            slots[slotNum].length = recordSize;
            memcpy((char *) page + sizeof(PageMsg), slots, sizeof(SlotElement) * pageMsg.slotCount);


            fileHandle.writePage(i, page);
            rid.pageNum = i;
            rid.slotNum = slotNum;
            flag = true;
            break;
        }
    }
    if (!flag) { //append a new page
        unsigned offset = PAGE_SIZE - recordSize;
        PageMsg pageMsg{1, 1, sizeof(SlotElement), offset};
        memcpy((char *) page + offset, data, recordSize);
        memcpy(page, &pageMsg, sizeof(pageMsg));
        SlotElement slotElement{recordSize, offset};
        memcpy((char *) page + sizeof(PageMsg), &slotElement, sizeof(slotElement));
        fileHandle.appendPage(page);
        rid.pageNum = fileHandle.getNumberOfPages() - 1;
        rid.slotNum = 0;
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    if (rid.pageNum >= fileHandle.getNumberOfPages()) return -1;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    PageMsg pageMsg{};
    memcpy(&pageMsg, page, sizeof(PageMsg));

    SlotElement slots[pageMsg.slotCount];
    memcpy(slots, (char *) page + sizeof(PageMsg), sizeof(SlotElement) * pageMsg.slotCount);

    unsigned offset = slots[rid.slotNum].offset;
    unsigned length = slots[rid.slotNum].length;
    if (length == 0) return -1;
    memcpy(data, (char *) page + offset, length);

    free(page);
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    PageMsg pageMsg{};
    memcpy(&pageMsg, page, sizeof(PageMsg));

    SlotElement slots[pageMsg.slotCount];
    memcpy(slots, (char *) page + sizeof(PageMsg), sizeof(SlotElement) * pageMsg.slotCount);

    //compact disk
    unsigned offset = slots[rid.slotNum].offset;
    unsigned length = slots[rid.slotNum].length;

    for (int i = rid.slotNum + 1; i < pageMsg.slotCount; i++) {
        unsigned currOffset = slots[i].offset;
        unsigned currLength = slots[i].length;
        memmove((char *) page + currOffset + length, (char *) page + currOffset, currLength);
    }

    pageMsg.freeEnd += length;
    pageMsg.tupleCount--;
    memcpy(page, &pageMsg, sizeof(pageMsg));
    slots[rid.slotNum].length = 0;
    memcpy((char *) page + sizeof(PageMsg), slots, sizeof(SlotElement) * pageMsg.slotCount);
    fileHandle.writePage(rid.pageNum, page);

    free(page);
    return 0;
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



