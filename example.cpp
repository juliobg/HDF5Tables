//C++ wrapper library for HDF5

#include <cassert>
#include <iostream>
#include <vector>
#include <string>       
#include <ctime>
#include "hdf5_wrapper.hpp"

using namespace std;
using namespace rgr;

#define TEST_SIZE 0
int64_t mytime[TEST_SIZE];
std::vector<double> values[TEST_SIZE];
int64_t mytime_read[TEST_SIZE];
std::vector<double> values_read[TEST_SIZE];

int main() {
    //Data to write
    vector<string> nonTimeColumnNames;
    nonTimeColumnNames.push_back("x1");
    nonTimeColumnNames.push_back("x2");
    nonTimeColumnNames.push_back("x3");
    nonTimeColumnNames.push_back("x4");
    nonTimeColumnNames.push_back("x5");
    for (int i = 0; i < TEST_SIZE; ++i) {
        mytime[i] = i;
        values[i].push_back(1.2 + i);
        values[i].push_back(1.23 + i);
        values[i].push_back(1.234 + i);
        values[i].push_back(1.2345 + i);
        values[i].push_back(1.23456 + i);
    }

    std::cout << "Writing data" << std::endl;
    clock_t begin = clock();
    H5TimeSeriesWriter writer("mytest.h5", "MyTable", nonTimeColumnNames);
    for (int i = 0; i < TEST_SIZE; ++i)
        writer.appendRow(mytime[i], values[i]);
    writer.reset("mytest2.h5", "mytaaa", nonTimeColumnNames);
//    for (int i = 0; i < TEST_SIZE; ++i)
//        writer.appendRow(mytime[i], values[i]);

    
    clock_t end = clock();
    double etime = (double) (end - begin) / CLOCKS_PER_SEC;
    std::cout << "Time: " << etime << std::endl;

    std::cout << "Reading data" << std::endl;
    begin = clock();

    H5TimeSeriesReader *preader=(H5TimeSeriesReader*) malloc (sizeof(H5TimeSeriesReader));
    try {new (preader) H5TimeSeriesReader ("mytest.h5");}
    catch (...) {/*do nothing*/}
    H5TimeSeriesReader& reader=*preader;
    std::cout << "Table: " << reader.tableName() << std::endl;
    std::vector <std::string> names = reader.nonTimeColumnNames();
    std::cout << "Columns (non time): " << std::endl;
    for (std::size_t i = 0; i < names.size(); i++)
        std::cout << names[i] << std::endl;
    std::vector<std::string> x;
    x.push_back("aaa");
    try {
        reader.subscribe(x);
    } catch (std::exception &e) {
        std::cout << "Testing a wrong column name: " << e.what() << endl;
    }
    x.clear();
    //  x.push_back ("time");
    x.push_back("x1");
    x.push_back("x2");
    x.push_back("x3");
    x.push_back("x4");
    x.push_back("x5");
   //  reader.subscribe_all();
    std::cout << "Rows: " << reader.numRows() << std::endl;
    for (int i = 0; i < TEST_SIZE; ++i) {
        reader.readRow(i, mytime_read[i], values_read[i]);
    }

    end = clock();
    etime = (double) (end - begin) / CLOCKS_PER_SEC;
    std::cout << "Time: " << etime << std::endl;

    //Test read and written values are exactly the same
    for (int i = 0; i < TEST_SIZE; ++i)
        if (mytime_read[i] != mytime[i] || values[i] != values_read [i])
            std::cout << "ERROR!" << std::endl;

    return 0;
}
