#include "hdfs.h" 
#include <unistd.h>
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>
#include <log4cplus/fileappender.h>

#define BUFFER_SIZE ( 1024 * 1024 ) 

using namespace log4cplus;

hdfsFS fs;
Logger logger = Logger::getInstance("main");

int tail(const char* readPath, tSize offset) {
    char buffer[BUFFER_SIZE];
    tSize size;
    int filesize;
    int length_for_reading;
    int result;

    /* Open file*/
    hdfsFile readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
    if( !readFile ) {
        LOG4CPLUS_ERROR(logger, "Failed to open "<<readPath<<" for reading");
        goto RETURN;
    }

    /* Get file size */
    filesize = hdfsAvailable(fs, readFile);
    LOG4CPLUS_DEBUG(logger, "filesize:" << filesize);
    if( 0 > filesize ) {
        LOG4CPLUS_ERROR(logger, "filesize is less than 0: " << filesize);
        goto FILE_CLOSE;
    }

    /* Compare file size and offset */
    length_for_reading = filesize - offset;
    if( length_for_reading <= 0 ) {
        LOG4CPLUS_ERROR(logger, "there is no length for reading: " << length_for_reading);
        result = -1;
        goto FILE_CLOSE;
    } else {
        int summed_size = 0;
        do {
            size = hdfsRead(fs, readFile, buffer, BUFFER_SIZE-1);
            if ( size < 0 ) {
                LOG4CPLUS_ERROR(logger, "read size is less than 0: " << size);
                result = -1;
                goto FILE_CLOSE;
            }
            summed_size += size;
            LOG4CPLUS_DEBUG(logger, "read size "
                <<size<<" total read size: "
                <<summed_size<<"/"<<length_for_reading);
        } while (summed_size < length_for_reading);
        result = filesize;
    }

FILE_CLOSE:
    hdfsCloseFile(fs, readFile);

RETURN:
    return result;
}

int main(int argc, char **argv) {
    SharedAppenderPtr append_1(
        new RollingFileAppender(LOG4CPLUS_TEXT("Test.log"), 1024*1024*1024*1024, 5));
    append_1->setName(LOG4CPLUS_TEXT("First"));
    append_1->setLayout( std::auto_ptr<Layout>(new TTCCLayout()) );
    Logger::getRoot().addAppender(append_1);
    if(2 != argc){
        printf("%s uri\n", argv[0]);
        exit(-1);
    }

    fs = hdfsConnectNewInstance("tloghd01-2.nm.nhnsystem.com", 9000);
    const char* readPath = argv[1];
    hdfsFile readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
    if(!readFile) {
        fprintf(stderr, "Failed to open %s for reading!\n", readPath);
        exit(-1);
    }
    int filesize = hdfsAvailable(fs, readFile);
    tOffset pos = filesize > 1024 ? filesize-1024: 0;
    int totalSize = filesize-pos;
    char buffer[BUFFER_SIZE];
    hdfsCloseFile(fs, readFile);

    while(1) {
        int size_read = 0;
        tSize size = 0;
        readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
        filesize = hdfsAvailable(fs, readFile);
        LOG4CPLUS_DEBUG(logger, "filesize:" << filesize);
        totalSize = filesize-pos;
        hdfsSeek(fs, readFile, pos); 
        if(totalSize <= 0){
            filesize = pos;
            LOG4CPLUS_DEBUG(logger, "Sleep totalsize:" << totalSize);
            sleep(1);
            goto CLOSE;
        }
        LOG4CPLUS_DEBUG(logger, "seek at "<<pos<<"/"<<filesize);

        size = hdfsRead(fs, readFile, buffer, BUFFER_SIZE-1);
        if(size == 0)
            goto CLOSE;
        size_read += size;
        LOG4CPLUS_DEBUG(logger, "read size "<<size<<" total read size: "<<size_read<<"/"<<totalSize);
        buffer[size] = '\0';
        fprintf(stdout, "%s", buffer);
        fflush(stdout);

        while(0 != size && totalSize > size_read) {
            tSize size = hdfsRead(fs, readFile, buffer, BUFFER_SIZE-1);
            size_read += size;
            LOG4CPLUS_DEBUG(logger, "read size "<<size<<" total read size: "<<size_read<<"/"<<totalSize);
            buffer[size] = '\0';
            fprintf(stdout, "%s", buffer);
            fflush(stdout);
        } 
CLOSE:
        pos = hdfsTell(fs, readFile);
        LOG4CPLUS_DEBUG(logger, "tell pos:"<<pos);
        hdfsCloseFile(fs, readFile);
    }
}

