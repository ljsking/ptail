#include <gtest/gtest.h>
#include <string>

using namespace std;


TEST(FactorialTest, Negative) {
    string prefix("hdfs://tloghd01-2.nm.nhnsystem.com:9000");
    string fullpath("hdfs://tloghd01-2.nm.nhnsystem.com:9000/scribedata/bmt/tmove09-1.nm/bmt-2011-10-06_00070");
    size_t prefix_size = prefix.length();
    size_t full_size = fullpath.length();
    string result = fullpath.substr( prefix_size, full_size - prefix_size );
    EXPECT_EQ("/scribedata/bmt/tmove09-1.nm/bmt-2011-10-06_00070", result);
}
