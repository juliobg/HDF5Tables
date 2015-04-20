//C++ wrapper library for HDF5

#pragma once

#include <array>
#include <exception>
#include <sstream>
#include <string>
#include <vector>

#include <boost/noncopyable.hpp>

#include <H5Cpp.h>

namespace rgr {
constexpr std::size_t kBufferSize = 4096;
constexpr hsize_t kChunkSize = 1024;
std::string getFieldName(std::size_t index);

class H5TimeSeriesWriter : boost::noncopyable {
public:
    H5TimeSeriesWriter() : mFlushFlag(false) {}
    H5TimeSeriesWriter(
                       std::string const& fileName,
                       std::string const& tableName,
                       std::vector<std::string> const& nonTimeColumnNames);
    void appendRow(int64_t time, std::vector<double> nonTimeValues);
    void reset();
    void reset(
            std::string const& fileName,
            std::string const& tableName,
            std::vector<std::string> const& nonTimeColumnNames);
    ~H5TimeSeriesWriter();

private:
    void appendChunkToColumn(H5::DataSet& column, hsize_t row, void *ptr, H5::DataType const& type, hsize_t ch_size);
    void writeAttrib(std::string const& attrib, std::string const& value, H5::H5File& locId);
    void writeAttrib(std::string const& attrib, hsize_t value, H5::H5File& locId);
    void openFile(
                 std::string const& fileName,
                 std::string const& tableName,
                 std::vector<std::string> const& nonTimeColumnNames);
    std::vector<H5::DataSet> mDset;
    std::vector<std::array<double, kChunkSize> > mData;
    std::array <int64_t, kChunkSize> mTimeData;
    hsize_t mChunkOffset;
    hsize_t mChunkBegin;
    hsize_t mNumRows;
    hsize_t mNumCols;
    H5::H5File mFileId;
    bool mFlushFlag;
}; // H5TimeSeriesWriter

class H5TimeSeriesReader : boost::noncopyable {
public:
    H5TimeSeriesReader(std::string const& fileName);
    ~H5TimeSeriesReader();
    std::string const& tableName() const;
    std::vector<std::string> nonTimeColumnNames() const;
    size_t numRows() const;
    bool readRow(std::size_t index, int64_t& time, std::vector<double>& nonTimeValues);
    void subscribe(std::vector<std::string> const& columnNames);
    void subscribeAll();

private:

    class ColumnB {
    public:

        ColumnB(std::string const& name) : mName(name), mLastIndex(0) { }

        void setDset(H5::DataSet ds) {
            mDset = std::move(ds);
            mDspace = mDset.getSpace();
        }

        void setName(std::string name) {
            mName = std::move(name);
        }

        const std::string& name() const {
            return mName;
        }

    protected:
        H5::DataSet mDset;
        H5::DataSpace mDspace;
        std::string mName;
        std::size_t mLastIndex;
        void readColumn(H5::DataSet const& column, void *ptr, H5::DataType const& type, hsize_t offset, hsize_t size);
    };

    template <typename T, H5::PredType const& H5T> class Column : public ColumnB {
    public:
        Column(std::string const& name) : ColumnB(name) { }

        T getValue(std::size_t index) {
            if (mData.empty()) {
                mData.resize(kBufferSize);
                //Force update
                mLastIndex=index+1;
            }
            if (mLastIndex + kBufferSize <= index || index<mLastIndex)
                updateBuffer(index); 
            return mData[index - mLastIndex];
        }

        void updateBuffer(std::size_t index) {
            readColumn(mDset, mData.data(), H5T, index, kBufferSize);
            mLastIndex = index;
        }
    private:
        std::vector<T> mData;
    };
    typedef Column<int64_t, H5::PredType::NATIVE_INT64> ColumnInt64;
    typedef Column<double, H5::PredType::NATIVE_DOUBLE> ColumnDouble;

    void readColumn(H5::DataSet const& column, void *ptr, H5::DataType const& type, hsize_t size) const;
    void readAttrib(std::string const& attrib, std::string& value, H5::H5File const& loc_id);
    void readAttrib(std::string const& attrib, hsize_t& value, H5::H5File const& loc_id);
    void doSubscr(std::size_t j);
    H5::DataSpace mDspace;
    ColumnInt64 mTimeColumn;
    std::vector<ColumnDouble> mColumn;
    std::string mTableName;
    H5::H5File mFileId;
    std::vector<std::size_t> mSubscrList;
    hsize_t mNumRows;
    hsize_t mNumCols;
}; // H5TimeSeriesReader

//Exception: column not found

class DuplicateColumn : public std::exception {
public:
    char const* what() const throw() {
        return str;
    }
private:
    static constexpr char str[] = "Duplicate column name";
};

class BadSubscription : public std::exception {
public:
    char const* what() const throw () {
        return str;
    }
private:
    static constexpr char str[]="Bad Subscription";
};

//Exception: we couldn't write in HDF5 file 

class BadWrite : public std::exception {
public:
    char const* what() const throw () {
        return str;
    }
private:
    static constexpr char str[]="Error writing HDF5 file";
};

//Exception: error reading HDF5 (e.g. attribute or data set not found)

class BadRead : public std::exception {
public:
    char const* what() const throw () {
        return str;
    }
private:
    static constexpr char str[]="Error reading HDF5 file";
};

} // rgr
