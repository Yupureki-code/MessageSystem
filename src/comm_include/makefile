test_comm:test_comm.cpp
	@g++ test_comm.cpp -o test_comm -std=c++17 -lspdlog -lfmt -letcd-cpp-api -lcpprest -lbrpc -lprotobuf -lgflags -lssl -lcrypto -lleveldb

test_es:test_es.cpp
	@g++ test_es.cpp -o test_es -std=c++17 -I/usr/local/include -L/usr/local/lib -lspdlog -lfmt -lelasticlient -lcpr -ljsoncpp -Wl,-rpath,/usr/local/lib -lssl -lcrypto

test_rabbitmq:test_rabbitmq.cpp
	@g++ test_rabbitmq.cpp -o test_rabbitmq -std=c++17 -I/usr/local/include -L/usr/local/lib -lspdlog -lfmt -lamqpcpp -lev -lpthread -Wl,-rpath,/usr/local/lib

clean:
	@rm -f test_comm test_es test_rabbitmq