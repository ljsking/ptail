all: ptail.cpp
	g++ -o ptail ptail.cpp -I/usr/java/latest/include/ -I/usr/java/latest/include/linux -I/usr/local/include -lhdfs -llog4cplus
