#include <exception>
#include <sstream>
#include <iostream>
#include <memory>
#include <coroutine>

struct Task
{
    struct promise_type
    {
        int value;
        std::exception_ptr ex;
        Task get_return_object()
        {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }
        std::suspend_always initial_suspend()
        {
            return {};
        }
        std::suspend_always final_suspend()noexcept
        {
            return {};
        }
        void return_value(int val)
        {
            value = val;
        }
        std::suspend_always yield_value(int val)
        {
            value = val;
            return {};
        }
        void unhandled_exception() {
            std::cout << "捕获到异常，存储起来\n";
            ex = std::current_exception(); // 捕获异常
        }
    };
    Task(std::coroutine_handle<promise_type> _handle)
    :handle(_handle)
    {}
    std::coroutine_handle<promise_type> handle;
    ~Task()
    {
        handle.destroy();
    }
    bool next()
    {
        if(!handle.done())
            handle.resume();
        if(handle.promise().ex)
        {
             std::rethrow_exception(handle.promise().ex);
        }
        return !handle.done();
    }
    int value()const {return handle.promise().value;}
};

Task func(int start,int end)
{
    for(int i = start;i<end;i++)
    {
        co_yield i;
        if(i == 3)
            throw "错误";
    }
    co_return 1;
}

int main()
{
    auto ret = func(1,5);
    try
    {
        while(ret.next())
        {
            std::cout<<ret.value()<<std::endl;
        }
    }
    catch(const std::exception& ex)
    {
        std::cout<<ex.what()<<std::endl;
    }
    std::cout<<ret.value()<<std::endl;
}