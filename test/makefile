server:server.cpp
	@g++ server.cpp -o server -std=c++20 -lpthread -lboost_system
client:client.cpp
	@g++ client.cpp -o client -std=c++20 -lpthread -lboost_system

.PHONY:clean
clean:
	@rm server client