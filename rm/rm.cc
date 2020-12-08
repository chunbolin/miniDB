#include "rm.h"

RelationManager *RelationManager::_relation_manager = nullptr;

RelationManager &RelationManager::instance() {
    if (!_relation_manager) {
        _relation_manager = new RelationManager();
    }
    return *_relation_manager;
}

RelationManager::RelationManager() {
    Attribute attr;

    //prepare Tables record descriptor
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    tablesRecordDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    tablesRecordDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    tablesRecordDescriptor.push_back(attr);

    //prepare Columns record descriptor
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsRecordDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    columnsRecordDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsRecordDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsRecordDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsRecordDescriptor.push_back(attr);
}

RelationManager::~RelationManager() = default;

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
    RC rc;

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    rc = rbfm.createFile(tablesFileName);
    assert(rc == rc::OK && "Create catalog 'Tables' file fail.");
    rc = rbfm.createFile(columnsFileName);
    assert(rc == rc::OK && "Create catalog 'Columns' file fail.");

    FileHandle fileHandle;
    rc = rbfm.openFile(tablesFileName, fileHandle);
    assert(rc == rc::OK && "Open catalog 'Tables' file fail.");

    return -1;
}

RC RelationManager::deleteCatalog() {
    return -1;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    RC rc;
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    rc = rbfm.createFile(tableName);
    if (rc != rc::OK) {
        return rc;
    }
    return rc::OK;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    RC rc;
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    rc = rbfm.destroyFile(tableName);
    if (rc != rc::OK) {
        return rc;
    }
    return rc::OK;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    return -1;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    RC rc;
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    rc = rbfm.openFile(tableName, fileHandle);
    if (rc != rc::OK) {
        return rc;
    }
    rbfm.insertRecord(fileHandle,)
    return rc::OK;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    return -1;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    return -1;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    return -1;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return -1;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    return -1;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
    return -1;
}