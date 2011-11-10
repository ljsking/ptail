#include "hdfs.h" 
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <iostream>
#include <boost/algorithm/string/join.hpp>

#define BUFFER_SIZE ( 1024 * 1024 * 10) 
#define ROTATE_SIZE ( 500000000 )

using namespace std;

hdfsFS fs;
string fs_prefix("hdfs://tloghd04-1.nm.nhnsystem.com:9000");

/*
@return: pos when success.
         -1: error
         -2: no more length to read.
*/
int tail(const char* readPath, tSize offset, char *buffer) {
    tSize size;
    int filesize;
    int length_for_reading;
    int result;

    /* Open file*/
    hdfsFile readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
    if( !readFile ) {
        goto RETURN;
    }

    /* Get file size */
    filesize = hdfsAvailable(fs, readFile);
    if( 0 > filesize ) {
        goto FILE_CLOSE;
    }

    /* Compare file size and offset */
    length_for_reading = filesize - offset;
    if( length_for_reading <= 0 ) {
        result = -2;
        goto FILE_CLOSE;
    } else {
        if( 0 > hdfsSeek(fs, readFile, offset) ) {
            result = -1;
            goto FILE_CLOSE;
        }
        int summed_size = 0;
        do {
            size = hdfsRead(fs, readFile, buffer, BUFFER_SIZE-1);
            if ( size == 0) {
                continue;
            }
            buffer[size] = '\0';
            cout << buffer;
            if ( size < 0 ) {
                result = -1;
                goto FILE_CLOSE;
            }
            summed_size += size;
        } while (summed_size < length_for_reading);
        result = filesize;
    }

FILE_CLOSE:
    hdfsCloseFile(fs, readFile);

RETURN:
    return result;
}

string getLastFile(const char *dir) {
    int numEntries;
    hdfsFileInfo *info = hdfsListDirectory(fs, dir, &numEntries);
    if( !info ) {
        return "";
    }
    string fullPath(info[numEntries-1].mName);
    hdfsFreeFileInfo(info, numEntries);
    string result = fullPath.substr(fs_prefix.length(), fullPath.length()-fs_prefix.length());
    fprintf(stderr, "Open file: %s\n",result.c_str());
    return result;
}

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

void nextFile(string file, vector<string> &rz)
{
    char buffer[10];
    vector<string> tokens = tokenize_str(file, "-");
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

string nextFullFile(string fullpath)
{
    fprintf(stderr, "guessing %s\n", fullpath.c_str());
    vector<string> path_tokens = tokenize_str(fullpath, "/");
    size_t len = path_tokens.size();
    size_t pos = len-1;
    string file = path_tokens[pos];
    vector<string> candidates;
    nextFile(file, candidates);
    if(2 != candidates.size()) 
        return fullpath;
    string candidate;
    path_tokens[pos] = candidates[0];
    candidate = "/"+boost::algorithm::join(path_tokens, "/");
    fprintf(stderr, "guessing candidate1 %s\n", candidate.c_str());
    if (0 == hdfsExists(fs, candidate.c_str()))
        return candidate;
    path_tokens[pos] = candidates[1];
    candidate = "/"+boost::algorithm::join(path_tokens, "/");
    fprintf(stderr, "guessing candidate2 %s\n", candidate.c_str());
    if (0 == hdfsExists(fs, candidate.c_str()))
        return candidate;
    return fullpath;
}


int main(int argc, char **argv) {
    char *buffer = new char[BUFFER_SIZE];
    if(2 != argc){
        printf("%s uri\n", argv[0]);
        exit(-1);
    }

    fs = hdfsConnectNewInstance("tloghd04-1.nm.nhnsystem.com", 9000);
    const char* readPath = argv[1];
    string fullpath = getLastFile(readPath);
    hdfsFile readFile = hdfsOpenFile(fs, fullpath.c_str(), O_RDONLY, 0, 0, 0);
    fprintf(stderr, "read! %s\n", fullpath.c_str());
    if(!readFile) {
        fprintf(stderr, "Failed to open %s for reading!\n", fullpath.c_str());
        exit(-1);
    }
    int filesize = hdfsAvailable(fs, readFile);
    tOffset offset = filesize > 1024 ? filesize-1024: 0;
    int totalSize = filesize-offset;
    hdfsCloseFile(fs, readFile);

    int many_sleep = 0;
    while(1) {
        tOffset newOffset = tail(fullpath.c_str(), offset, buffer);
        offset = std::max(newOffset, offset);
        if ( -2 == newOffset ) {
            if ( many_sleep > 30 || offset > ROTATE_SIZE ) {
                many_sleep = 0;
                string new_fullpath = nextFullFile(fullpath);
                if (new_fullpath != "" && new_fullpath != fullpath) {
                    fprintf(stderr, "change path from %s to %s\n", fullpath.c_str(), new_fullpath.c_str());
                    fullpath = new_fullpath;
                    offset = 0;
                    continue;
                }
            }
            ++many_sleep;
            usleep(100000);
        } else {
            many_sleep = 0;
        }
    }
    delete [] buffer;
}

