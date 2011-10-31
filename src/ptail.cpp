#include "hdfs.h" 
#include <unistd.h>
#include <algorithm>
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>
#include <log4cplus/fileappender.h>

#define BUFFER_SIZE ( 1024 * 1024 * 10) 
#define ROTATE_SIZE ( 500000000 )

using namespace log4cplus;
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
                string new_fullpath = getLastFile(readPath);
                if (new_fullpath != "" && new_fullpath != fullpath) {
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

