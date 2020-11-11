
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

int getRecordSize(const std::vector<Attribute> &recordDescriptor, const void *data) {
    int attrSize = recordDescriptor.size();
    int curr = ceil(((double) attrSize) / 8);
    bool nullInfo[attrSize];
    getNullInfo(recordDescriptor, data, nullInfo);
    for (int i = 0; i < attrSize; ++i) {
        Attribute attr = recordDescriptor.at(i);
        if (nullInfo[i])
            continue;
        else {
            if (attr.type == TypeInt) {
                curr += 4;
            } else if (attr.type == TypeReal) {
                curr += 4;
            } else if (attr.type == TypeVarChar) {
                int len;
                memcpy(&len, (char *) data + curr, sizeof(int));
                curr += 4;
                curr += len;
            }
        }
    }
    return curr;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    int recordSize = getRecordSize(recordDescriptor, data);

    // since we may need to place tombstone when update, so recode size have to be >= sizeof(RID)
    if (recordSize < sizeof(RID)) recordSize = sizeof(RID);
    bool flag = false;
    void *page = malloc(PAGE_SIZE);
    unsigned pageNum = fileHandle.getNumberOfPages();
    short freeList[pageNum + 1];
    fileHandle.readFreeList(freeList);
    for (int i = pageNum - 1; i >= 0; i--) {
        if (freeList[i] >= recordSize + sizeof(SlotElement)) {

            fileHandle.readPage(i, page);
            PageMsg pageMsg{};
            memcpy(&pageMsg, page, sizeof(PageMsg));

            SlotElement slots[pageMsg.slotCount + 1];
            memcpy(slots, (char *) page + sizeof(PageMsg), sizeof(SlotElement) * pageMsg.slotCount);
            //find unused slot, if no such slot, append a new one
            int slotNum = 0;
            bool needAddSlot = false;
            for (; slotNum < pageMsg.slotCount + 1; ++slotNum) {
                if (slotNum == pageMsg.slotCount) {
                    needAddSlot = true;
                    break;
                }
                if (slots[slotNum].length == Unused) break;
            }

            int offset = pageMsg.freeEnd - recordSize + 1;

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
            freeList[i] = pageMsg.freeEnd - pageMsg.freeStart + 1;

            rid.pageNum = i;
            rid.slotNum = slotNum;
            flag = true;
            break;
        }
    }
    if (!flag) { //append a new page
        int offset = PAGE_SIZE - recordSize;
        PageMsg pageMsg{1, 1, sizeof(pageMsg) + sizeof(SlotElement), offset - 1};
        memcpy((char *) page + offset, data, recordSize);
        memcpy(page, &pageMsg, sizeof(pageMsg));
        SlotElement slotElement{recordSize, offset};
        memcpy((char *) page + sizeof(PageMsg), &slotElement, sizeof(slotElement));

        fileHandle.appendPage(page);
        freeList[pageNum] = pageMsg.freeEnd - pageMsg.freeStart + 1;

        rid.pageNum = fileHandle.getNumberOfPages() - 1;
        rid.slotNum = 0;
    }
    fileHandle.writeFreeList(freeList, flag ? pageNum : pageNum + 1);
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

    int offset = slots[rid.slotNum].offset;
    int length = slots[rid.slotNum].length;
    if (length == Unused) {
        return -1;
    } else if (length == Tombstone) { //if it's tombstone
        RID tombstoneRid;
        memcpy(&tombstoneRid, (char *) page + offset, sizeof(RID));
        readRecord(fileHandle, recordDescriptor, tombstoneRid, data);
    } else { //else we direct read it
        memcpy(data, (char *) page + offset, length);
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    unsigned pageNum = fileHandle.getNumberOfPages();
    short freeList[pageNum];
    fileHandle.readFreeList(freeList);
    PageMsg pageMsg{};
    memcpy(&pageMsg, page, sizeof(PageMsg));

    SlotElement slots[pageMsg.slotCount];
    memcpy(slots, (char *) page + sizeof(PageMsg), sizeof(SlotElement) * pageMsg.slotCount);

    //compact disk
    int offset = slots[rid.slotNum].offset;
    int length = slots[rid.slotNum].length;
    if (length == Tombstone) { //if it's tombstone
        RID tombstoneRid;
        memcpy(&tombstoneRid, (char *) page + offset, sizeof(RID));
        deleteRecord(fileHandle, recordDescriptor, tombstoneRid);
    } else { //else we direct delete it
        for (int i = rid.slotNum + 1; i < pageMsg.slotCount; i++) {
            int currOffset = slots[i].offset;
            int currLength = slots[i].length;
            memmove((char *) page + currOffset + length, (char *) page + currOffset, currLength);
            slots[i].offset = currOffset + length;
        }

        pageMsg.freeEnd += length;
        pageMsg.tupleCount--;
        memcpy(page, &pageMsg, sizeof(pageMsg));
        slots[rid.slotNum].length = Unused;
        memcpy((char *) page + sizeof(PageMsg), slots, sizeof(SlotElement) * pageMsg.slotCount);
        fileHandle.writePage(rid.pageNum, page);
        freeList[rid.pageNum] = pageMsg.freeEnd - pageMsg.freeStart + 1;
    }

    fileHandle.writeFreeList(freeList, pageNum);
    free(page);
    return 0;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    int attrSize = recordDescriptor.size();
    int curr = ceil(((double) attrSize) / 8);
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
    std::cout << std::endl;

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    if (rid.pageNum >= fileHandle.getNumberOfPages()) return -1;

    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    unsigned pageNum = fileHandle.getNumberOfPages();
    short freeList[pageNum];
    fileHandle.readFreeList(freeList);
    PageMsg pageMsg{};
    memcpy(&pageMsg, page, sizeof(PageMsg));

    SlotElement slots[pageMsg.slotCount];
    memcpy(slots, (char *) page + sizeof(PageMsg), sizeof(SlotElement) * pageMsg.slotCount);

    int offset = slots[rid.slotNum].offset;
    int length = slots[rid.slotNum].length;
    if (length == Unused) return -1;

    int newLength = getRecordSize(recordDescriptor, data);
    // since we may need to place tombstone when update, so recode size have to be >= sizeof(RID)
    if (newLength < sizeof(RID)) newLength = sizeof(RID);

    if (newLength == length) {
        //direct update tuple
        memcpy((char *) page + offset, data, length);
    } else if (newLength < length) {
        int moveOffset = length - newLength;
        memcpy((char *) page + offset + moveOffset, data, newLength);
        slots[rid.slotNum].length = newLength;
        slots[rid.slotNum].offset = offset + moveOffset;
        //compact disk
        for (int i = rid.slotNum + 1; i < pageMsg.slotCount; i++) {
            int currOffset = slots[i].offset;
            int currLength = slots[i].length;
            memmove((char *) page + currOffset + moveOffset, (char *) page + currOffset, currLength);
            slots[i].offset = currOffset + moveOffset;
        }

        pageMsg.freeEnd += moveOffset;
    } else {
        int freeSpace = pageMsg.freeStart - pageMsg.freeEnd + 1;
        if (freeSpace + length >= newLength) {
            int moveOffset = newLength - length;
            //compact disk
            for (int i = pageMsg.slotCount - 1; i >= rid.slotNum + 1; i--) {
                int currOffset = slots[i].offset;
                int currLength = slots[i].length;
                memmove((char *) page + currOffset - moveOffset, (char *) page + currOffset, currLength);
                slots[i].offset = currOffset + moveOffset;
            }
            memcpy((char *) page + offset - moveOffset, data, newLength);
            slots[rid.slotNum].length = newLength;
            slots[rid.slotNum].offset = offset - moveOffset;
            pageMsg.freeEnd -= moveOffset;
        } else {
            //insert record, since current page don't have enough space, this record won't be in current page
            RID insertRid;
            insertRecord(fileHandle, recordDescriptor, data, insertRid);

            int tombstoneLen = sizeof(RID);
            int moveOffset = length - tombstoneLen;
            memcpy((char *) page + offset + moveOffset, &insertRid, tombstoneLen);
            slots[rid.slotNum].length = Tombstone;
            slots[rid.slotNum].offset = offset + moveOffset;
            //compact disk
            for (int i = rid.slotNum + 1; i < pageMsg.slotCount; i++) {
                int currOffset = slots[i].offset;
                int currLength = slots[i].length;
                memmove((char *) page + currOffset + moveOffset, (char *) page + currOffset, currLength);
                slots[i].offset = currOffset + moveOffset;
            }
            pageMsg.freeEnd += moveOffset;
        }


    }
    //maintain pageMsg
    memcpy(page, &pageMsg, sizeof(pageMsg));

    //maintain slots
    memcpy((char *) page + sizeof(PageMsg), slots, sizeof(SlotElement) * pageMsg.slotCount);
    fileHandle.writePage(rid.pageNum, page);

    //maintain freeList
    freeList[rid.pageNum] = pageMsg.freeEnd - pageMsg.freeStart + 1;
    fileHandle.writeFreeList(freeList, pageNum);

    free(page);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    if (rid.pageNum >= fileHandle.getNumberOfPages()) return -1;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    PageMsg pageMsg{};
    memcpy(&pageMsg, page, sizeof(PageMsg));

    SlotElement slots[pageMsg.slotCount];
    memcpy(slots, (char *) page + sizeof(PageMsg), sizeof(SlotElement) * pageMsg.slotCount);

    int offset = slots[rid.slotNum].offset;
    int length = slots[rid.slotNum].length;
    if (length == Unused) {
        return -1;
    } else if (length == Tombstone) { //if it's tombstone
        RID tombstoneRid;
        memcpy(&tombstoneRid, (char *) page + offset, sizeof(RID));
        readAttribute(fileHandle, recordDescriptor, tombstoneRid, attributeName, data);
    } else { //else we direct read it
        void *record = malloc(length);
        memcpy(record, (char *) page + offset, length);
        int attrSize = recordDescriptor.size();
        int curr = ceil(((double) attrSize) / 8);
        bool nullInfo[attrSize];
        getNullInfo(recordDescriptor, data, nullInfo);
        for (int i = 0; i < attrSize; ++i) {
            Attribute attr = recordDescriptor.at(i);
            if (nullInfo[i])
                continue;
            else {
                if (attr.type == TypeInt) {
                    if (attr.name == attributeName) {
                        memcpy(&data, (char *) record + curr, sizeof(int));
                        break;
                    }
                    curr += 4;
                } else if (attr.type == TypeReal) {
                    if (attr.name == attributeName) {
                        memcpy(&data, (char *) record + curr, sizeof(int));
                        break;
                    }
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
        std::cout << std::endl;
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}



