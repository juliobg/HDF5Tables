//C++ wrapper library for HDF5

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include "hdf5_wrapper.hpp"

using namespace std;
using namespace rgr;
using namespace H5;

namespace {
const char kTableNameStr[] = "TABLE_NAME";
const char kTimeStr[] = "time";
const char NROWS_STR[] = "NROWS";
const char kNcolumnsStr[] = "NCOLUMNS";
const char kFieldNameStr1[] = "FIELD_";
const char kFieldNameStr2[] = "_NAME";

constexpr size_t kMaxStr = 1024; //Max string length (column or table name)

}

namespace rgr {

constexpr char DuplicateColumn::str[];
constexpr char BadSubscription::str[];
constexpr char BadRead::str[];   
constexpr char BadWrite::str[];

//Write HDF5 file

H5TimeSeriesWriter::H5TimeSeriesWriter(
        string const& fileName,
        string const& tableName,
        vector<string> const& nonTimeColumnNames)
: mData(nonTimeColumnNames.size())
, mChunkOffset(0), mChunkBegin(0), mNumRows(0), mNumCols(1 + nonTimeColumnNames.size())
, mFlushFlag(true) {
    openFile (fileName, tableName, nonTimeColumnNames);
}

void H5TimeSeriesWriter::openFile (
        string const& fileName,
        string const& tableName,
        vector<string> const& nonTimeColumnNames) {
    FileAccPropList plistf;
    plistf.setCache (10240, 0, 0, 0);
    H5File FileId2(fileName.c_str(), H5F_ACC_TRUNC, FileCreatPropList::DEFAULT, plistf);
    mFileId=FileId2;

    unordered_set<string> unique(nonTimeColumnNames.begin(), nonTimeColumnNames.end());
    if (unique.size() != nonTimeColumnNames.size())
        throw DuplicateColumn();
    unique.clear();
    //Write table name
    writeAttrib(kTableNameStr, tableName, mFileId);

    //Write column names
    writeAttrib(getFieldName(0), kTimeStr, mFileId);
    for (size_t i = 0; i < nonTimeColumnNames.size(); ++i)
        writeAttrib(getFieldName(i + 1), nonTimeColumnNames[i], mFileId);

    //Create chunked datasets
    hsize_t dims = 0;//kChunkSize;
    hsize_t max_dims = H5S_UNLIMITED;
    DataSpace column_space(1, &dims, &max_dims);
    hsize_t chunk_size = kChunkSize;
    DSetCreatPropList plist;
    plist.setChunk(1, &chunk_size);

    DataSet dset = mFileId.createDataSet(kTimeStr, PredType::NATIVE_INT64, column_space, plist);
    mDset.push_back(dset);
    for (string const& name : nonTimeColumnNames) {
        dset = mFileId.createDataSet(
                name.c_str(),
                PredType::NATIVE_DOUBLE,
                column_space,
                plist);
        mDset.push_back(dset);
    }
}

void H5TimeSeriesWriter::reset(
        string const& fileName,
        string const& tableName,
        vector<string> const& nonTimeColumnNames) {
    reset();
    mData.resize(nonTimeColumnNames.size());
    mNumCols=1 + nonTimeColumnNames.size();
    openFile (fileName, tableName, nonTimeColumnNames);
    mFlushFlag=true;
}

//Read HDF5 file

H5TimeSeriesReader::H5TimeSeriesReader(string const& fileName)
: mTimeColumn(kTimeStr) {
    string name;

    FileAccPropList plist;
    plist.setCache (10240, 0, 0, 0);
    mFileId.openFile (fileName.c_str(), H5F_ACC_RDONLY, plist);

    //Read table name, number of rows and number of columns
    readAttrib(kTableNameStr, mTableName, mFileId);
    readAttrib(NROWS_STR, mNumRows, mFileId);
    readAttrib(kNcolumnsStr, mNumCols, mFileId);

    //This should never happen
    assert (mNumRows<=numeric_limits<size_t>::max());

    readAttrib(getFieldName(0), name, mFileId);
    if (name != mTimeColumn.name())
        throw BadSubscription();
    mTimeColumn.setDset(mFileId.openDataSet(name));

    //Open datasets
    for (size_t i = 1; i < mNumCols; i++) {
        readAttrib(getFieldName(i), name, mFileId);
        mColumn.emplace_back(name);
    }
}

//Return table name

string const& H5TimeSeriesReader::tableName() const {
    return mTableName;
}

bool H5TimeSeriesReader::readRow(
        size_t index, int64_t& time, vector<double>& nonTimeValues) {
    if (index >= mNumRows)
        return false;

    time = mTimeColumn.getValue(index);

    //Read non time columns
    nonTimeValues.clear();
    nonTimeValues.reserve(mSubscrList.size());
    for (size_t fieldIdx : mSubscrList)
        nonTimeValues.push_back(mColumn[fieldIdx].getValue(index));
    return true;
}

//Subscribe to a set of columns

void H5TimeSeriesReader::subscribe(vector<string> const& columnNames) {
    size_t i;
    for (string const& columnName : columnNames) {
        if (columnName == kTimeStr)
            throw BadSubscription();
        for (i = 0; i < mColumn.size(); ++i) {
            if (columnName == mColumn[i].name()) {
                doSubscr(i);
                break;
            }
        }
        if (i == mColumn.size())
            throw BadSubscription();
    }
}

//Subscribe to all columns

void H5TimeSeriesReader::subscribeAll() {
    for (size_t i = 0; i < mColumn.size(); ++i) 
        doSubscr(i); 
}

//Helper function for subscribe() and subscribeAll()

void H5TimeSeriesReader::doSubscr(std::size_t i) {
    if (find(mSubscrList.begin(), mSubscrList.end(), i) == mSubscrList.end()) {
        mColumn[i].setDset(mFileId.openDataSet(mColumn[i].name()));
        mSubscrList.push_back(i);
    }
}


//Return non time column names
vector <string> H5TimeSeriesReader::nonTimeColumnNames() const {
    vector <string> v;

    for (auto const& column : mColumn) {
        v.push_back(column.name());
    }

    return v;
}

//Append row to HDF5 file

void H5TimeSeriesWriter::appendRow(int64_t time, vector<double> nonTimeValues) {
    if (!mFlushFlag)
        throw BadWrite();
    if (nonTimeValues.size() != mNumCols - 1)
        throw BadWrite();

    //Write in memory
    mTimeData[mChunkOffset] = time;

    for (size_t i = 0; i < nonTimeValues.size(); ++i)
        mData[i][mChunkOffset] = nonTimeValues[i];

    //If we reach chunk size, write to disk
    mChunkOffset++;
    mNumRows++;
    if (mChunkOffset == kChunkSize) {
        appendChunkToColumn(mDset[0], mChunkBegin, mTimeData.data(), PredType::NATIVE_INT64, kChunkSize);
        for (size_t i = 0; i < nonTimeValues.size(); ++i)
            appendChunkToColumn(mDset[i + 1], mChunkBegin, mData[i].data(), PredType::NATIVE_DOUBLE, kChunkSize);

        mChunkOffset = 0;
        mChunkBegin = mNumRows;
    }

    //This should never happen
    assert (mNumRows<=numeric_limits<size_t>::max());
}

//flush needs to be called in order to finish file writing

void H5TimeSeriesWriter::reset() {
    //Don't flush if we have done it already
    if (!mFlushFlag)
        return;
    //Write number of rows
    writeAttrib(NROWS_STR, mNumRows, mFileId);

    //Write number of columns
    writeAttrib(kNcolumnsStr, mNumCols, mFileId);

    //Write current chunk to disk, even if it's not filled
    if (mChunkOffset!=0) {
    appendChunkToColumn(mDset[0], mChunkBegin, mTimeData.data(), PredType::NATIVE_INT64, mChunkOffset);
    for (size_t i = 0; i < mNumCols - 1; ++i)
        appendChunkToColumn(mDset[i + 1], mChunkBegin, mData[i].data(), PredType::NATIVE_DOUBLE, mChunkOffset);
    }

    for (auto& dset : mDset)
        dset.close();
    mFileId.close();

    mData.clear();
    mDset.clear();
    mChunkOffset=0; 
    mChunkBegin=0; 
    mNumRows=0; 
    mNumCols=0;
    mFlushFlag = false;
}

//Get number of rows 

size_t H5TimeSeriesReader::numRows() const {
    return mNumRows;
}

//Append memory chunk to column on disk

void H5TimeSeriesWriter::appendChunkToColumn(
        DataSet& column, hsize_t row, void *ptr, DataType const& type, hsize_t ch_size) {
    DataSpace space = column.getSpace();

    // This should never happen
    assert (space.getSimpleExtentNdims() == 1);

    hsize_t currsize;
    space.getSimpleExtentDims(&currsize);

    if (currsize <= row + ch_size - 1) {
        currsize += ch_size;
        column.extend(&currsize);
        space = column.getSpace();
        space.getSimpleExtentDims(&currsize);
        assert(currsize > row + ch_size - 1);
    }

    hsize_t dimmemspace = ch_size;
    DataSpace memspace(1, &dimmemspace, NULL);
    hsize_t count = ch_size;

    space.selectHyperslab(H5S_SELECT_SET, &count, &row);
    column.write(ptr, type, memspace, space);
}

void H5TimeSeriesReader::ColumnB::readColumn(
        DataSet const& column, void *ptr, DataType const& type, hsize_t index, hsize_t size) {
    DataSpace space = column.getSpace();
    if (space.getSimpleExtentNdims() != 1)
        throw BadRead();

    hsize_t currsize;
    space.getSimpleExtentDims(&currsize);
    if (index + size > currsize) {
        size = currsize - index;
    }

    hsize_t dimmemspace = size;
    DataSpace memspace(1, &dimmemspace, NULL);
    hsize_t count = size;
    space.selectHyperslab(H5S_SELECT_SET, &count, &index);
    column.read(ptr, type, memspace, space);
}

//Write string attribute

void H5TimeSeriesWriter::writeAttrib(
        string const& attrib, string const& value, H5File& locId) {
    DataSpace att_space(H5S_SCALAR);
    StrType vls_type_c_id(PredType::C_S1, kMaxStr);
    Group gid = locId.openGroup("/");

    Attribute att = gid.createAttribute(attrib.c_str(), vls_type_c_id, att_space);

    //We need to pass exactly a buffer of length MAX_STR to Attribute::write()
    char str[kMaxStr];
    strncpy(str, value.c_str(), kMaxStr);

    att.write(vls_type_c_id, str);
}

//Write hsize_t attribute

void H5TimeSeriesWriter::writeAttrib(
        string const& attrib, hsize_t value, H5File& locId) {
    DataSpace att_space(H5S_SCALAR);
    IntType inttype(PredType::NATIVE_HSIZE);
    Group gid = locId.openGroup("/");

    Attribute att = gid.createAttribute(attrib.c_str(), inttype, att_space);

    att.write(inttype, &value);
}

//Read string attribute

void H5TimeSeriesReader::readAttrib(
        string const& attrib, string& value, H5File const& locId) {
    char attr [kMaxStr];

    StrType vls_type_c_id(PredType::C_S1, kMaxStr);
    Group gid = locId.openGroup("/");
    Attribute att = gid.openAttribute(attrib.c_str());
    att.read(vls_type_c_id, attr);

    value = attr;
}

//Read hsize_t attribute

void H5TimeSeriesReader::readAttrib(
        string const& attrib, hsize_t& value, H5File const& locId) {
    IntType inttype(PredType::NATIVE_HSIZE);
    Group gid = locId.openGroup("/");
    Attribute att = gid.openAttribute(attrib.c_str());
    att.read(inttype, &value);
}

string getFieldName(size_t index) {
    stringstream out;
    out << kFieldNameStr1 << index << kFieldNameStr2;

    return out.str();
}

//H5TimeSeriesWriter destructor

H5TimeSeriesWriter::~H5TimeSeriesWriter() {
    reset();
}

//H5TimeSeriesReader destructor

H5TimeSeriesReader::~H5TimeSeriesReader() {
    mFileId.close();
}

} // rgr
