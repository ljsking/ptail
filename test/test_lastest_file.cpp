#include <gtest/gtest.h>
#include <string>
#include <boost/algorithm/string/join.hpp>

using namespace std;

std::vector<std::string> tokenize_str(const std::string & str,
                                      const std::string & delims=", \t")
{
  using namespace std;
  // Skip delims at beginning, find start of first token
  string::size_type lastPos = str.find_first_not_of(delims, 0);
  // Find next delimiter @ end of token
  string::size_type pos     = str.find_first_of(delims, lastPos);

  // output vector
  vector<string> tokens;

  while (string::npos != pos || string::npos != lastPos)
    {
      // Found a token, add it to the vector.
      tokens.push_back(str.substr(lastPos, pos - lastPos));
      // Skip delims.  Note the "not_of". this is beginning of token
      lastPos = str.find_first_not_of(delims, pos);
      // Find next delimiter at end of token.
      pos     = str.find_first_of(delims, lastPos);
    }

  return tokens;
}

TEST(FactorialTest, Negative)
{
    string prefix("hdfs://tloghd01-2.nm.nhnsystem.com:9000");
    string fullpath("hdfs://tloghd01-2.nm.nhnsystem.com:9000/scribedata/bmt/tmove09-1.nm/bmt-2011-10-06_00070");
    size_t prefix_size = prefix.length();
    size_t full_size = fullpath.length();
    string result = fullpath.substr( prefix_size, full_size - prefix_size );
    EXPECT_EQ("/scribedata/bmt/tmove09-1.nm/bmt-2011-10-06_00070", result);
}

void NextFile(string str, vector<string> &rz)
{
    char buffer[10];
    vector<string> tokens = tokenize_str(str, "-");
    size_t length = tokens.size();
    /* token은 총 3개 이상 나옴. (3개는 카테고리가 없을때)
    * -1: day와 log_number
    * -2: month
    * -3: year
    * 0:-4 : category
    */
    if (3 > tokens.size())
        return;
    vector<string> day_tokens = tokenize_str(tokens[length-1], "_");

    if (2 != day_tokens.size())
        return;

    int log_number = atoi(day_tokens[1].c_str());
    snprintf(buffer, 10, "%05d", log_number+1);
    day_tokens[1] = string(buffer);
    tokens[length-1] = boost::algorithm::join(day_tokens, "_");
    string next_log = boost::algorithm::join(tokens, "-");
    rz.push_back(next_log);

    int year = atoi(tokens[length-3].c_str());
    int month = atoi(tokens[length-2].c_str());
    int day = atoi(day_tokens[0].c_str());

    long tme;
    struct tm when;
    time(&tme);
    when = *localtime(&tme);
    when.tm_year = year - 1900;
    when.tm_mon = month - 1;
    when.tm_mday = day + 1;
    mktime(&when);

    snprintf(buffer, 10, "%04d", when.tm_year+1900);
    tokens[length-3] = string(buffer);
    snprintf(buffer, 10, "%02d", when.tm_mon+1);
    tokens[length-2] = string(buffer);
    snprintf(buffer, 10, "%02d", when.tm_mday);
    day_tokens[0] = string(buffer);
    snprintf(buffer, 10, "%05d", 0);
    day_tokens[1] = string(buffer);

    tokens[length-1] = boost::algorithm::join(day_tokens, "_");
    string next_day_log = boost::algorithm::join(tokens, "-");
    rz.push_back(next_day_log);
}

TEST(GetFileNumber, Simple)
{
    vector<string> rz;
    string str("wmtad_trcvmail01-2.nm-2011-11-10_00066");
    NextFile(str, rz);
    EXPECT_EQ("wmtad_trcvmail01-2.nm-2011-11-10_00067", rz[0]);    
    EXPECT_EQ("wmtad_trcvmail01-2.nm-2011-11-11_00000", rz[1]);    
    rz.clear();
    str = "wmtad_trcvmail01-2.nm-2011-11-30_00006";
    NextFile(str, rz);
    EXPECT_EQ("wmtad_trcvmail01-2.nm-2011-11-30_00007", rz[0]);    
    EXPECT_EQ("wmtad_trcvmail01-2.nm-2011-12-01_00000", rz[1]);    
    rz.clear();
    str = "mda-2011-11-10_00034";
    NextFile(str, rz);
    EXPECT_EQ("mda-2011-11-10_00035", rz[0]);    
    EXPECT_EQ("mda-2011-11-11_00000", rz[1]);    
    rz.clear();
    str = "2011-11-10_00034";
    NextFile(str, rz);
    EXPECT_EQ("2011-11-10_00035", rz[0]);    
    EXPECT_EQ("2011-11-11_00000", rz[1]);    
    rz.clear();
    str = "2011-11-10";
    NextFile(str, rz);
    EXPECT_EQ(0, rz.size());    
    rz.clear();
    str = "2011";
    NextFile(str, rz);
    EXPECT_EQ(0, rz.size());    
    rz.clear();
}
