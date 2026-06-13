test: test.cpp file.pb.cc ../../service/comm.pb.cc
	@g++ -std=c++17 -O2 -o test \
		test.cpp file.pb.cc ../../service/comm.pb.cc \
		-I../../comm_include -I../../proto \
		-lspdlog -lfmt -lbrpc -lprotobuf -lgflags -lgtest -lgtest_main -lpthread \
		-lssl -lcrypto -lleveldb

clean:
	@rm -f test
