#include "comm.hpp"
#include "etcd.hpp"
#include "channel.hpp"
#include <iostream>
#include <thread>
#include <chrono>

using namespace messageSystem;

void test_logger()
{
    std::cout << "=== Testing Logger Module ===" << std::endl;
    initLogger("test_logger", "", spdlog::level::debug);
    
    LOG_DEBUG("This is a debug message: {}", 42);
    LOG_INFO("This is an info message: {}", "hello");
    LOG_WARNING("This is a warning message: {:.2f}", 3.14);
    LOG_ERROR("This is an error message: {}", "test");
    
    std::cout << "[PASS] Logger test completed!" << std::endl << std::endl;
}

void test_etcd()
{
    std::cout << "=== Testing Etcd Module ===" << std::endl;
    
    try {
        std::cout << "Testing Registrant..." << std::endl;
        Registrant registrant("127.0.0.1:2379");
        
        bool reg_result = registrant.Register("/test/service1", "127.0.0.1:8080");
        if (reg_result) {
            std::cout << "[PASS] Registration successful!" << std::endl;
        } else {
            std::cout << "[FAIL] Registration failed!" << std::endl;
        }
        
        std::cout << "Testing Discovery..." << std::endl;
        auto put_callback = [](std::string key, std::string value) {
            std::cout << "PUT event: " << key << " = " << value << std::endl;
        };
        auto del_callback = [](std::string key, std::string value) {
            std::cout << "DELETE event: " << key << " = " << value << std::endl;
        };
        
        Discovery discovery("127.0.0.1:2379", "/test", put_callback, del_callback);
        std::cout << "[PASS] Discovery initialized!" << std::endl;
        
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
    } catch (const std::exception& e) {
        std::cout << "[FAIL] Etcd test failed: " << e.what() << std::endl;
    }
    
    std::cout << "Etcd test completed!" << std::endl << std::endl;
}

void test_channel()
{
    std::cout << "=== Testing Channel Module ===" << std::endl;
    
    try {
        std::cout << "Testing ServiceChannel..." << std::endl;
        ServiceChannel channel("test_service");
        
        ServiceChannel::ChannelPtr chosen;
        bool choose_empty = channel.chooseChannel(chosen);
        std::cout << "Choose from empty: " << (choose_empty ? "unexpected success" : "correctly failed") << std::endl;
        
        bool add1 = channel.addChannel("127.0.0.1:8080");
        std::cout << "Add channel 1: " << (add1 ? "success" : "failed") << std::endl;
        
        bool add2 = channel.addChannel("127.0.0.1:8081");
        std::cout << "Add channel 2: " << (add2 ? "success" : "failed") << std::endl;
        
        bool choose_ok = channel.chooseChannel(chosen);
        std::cout << "Choose channel: " << (choose_ok ? "success" : "failed") << std::endl;
        
        channel.removeChannel("127.0.0.1:8080");
        std::cout << "Channel removed!" << std::endl;
        
        std::cout << "Testing ServiceManager..." << std::endl;
        ServiceManager manager;
        
        manager.activeService("service1");
        std::cout << "Service activated!" << std::endl;
        
        bool add_service = manager.addService("service1", "127.0.0.1:9090");
        std::cout << "Add service: " << (add_service ? "success" : "failed") << std::endl;
        
        ServiceChannel::ChannelPtr service_channel;
        bool choose_result = manager.chooseService("service1", service_channel);
        std::cout << "Choose service: " << (choose_result ? "success" : "failed") << std::endl;
        
        manager.inactiveService("service1");
        std::cout << "Service inactivated!" << std::endl;
        
        bool choose_inactive = manager.chooseService("service1", service_channel);
        std::cout << "Choose inactive service: " << (choose_inactive ? "unexpected success" : "correctly failed") << std::endl;
        
        std::cout << "[PASS] Channel test completed!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cout << "[FAIL] Channel test failed: " << e.what() << std::endl;
    }
    
    std::cout << std::endl;
}

int main()
{
    std::cout << "Starting comm module tests..." << std::endl << std::endl;
    
    test_logger();
    test_etcd();
    test_channel();
    
    std::cout << "All tests completed!" << std::endl;
    return 0;
}