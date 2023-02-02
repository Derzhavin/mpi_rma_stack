//
// Created by denis on 22.01.23.
//

#ifndef SOURCES_RMATREIBERSTACK_HPP
#define SOURCES_RMATREIBERSTACK_HPP

#include <iostream>
#include <stack>

#include "IStack.hpp"

namespace rma_treiber_stack
{
    template<typename T>
    class RmaTreiberStack: public stack_interface::IStack<RmaTreiberStack<T>>
    {
        friend class stack_interface::IStack_traits<rma_treiber_stack::RmaTreiberStack<T>>;
        typedef typename stack_interface::IStack_traits<RmaTreiberStack>::ValueType ValueType;

    private:
        void pushImpl(T& data)
        {
            std::cout << "push\n";
            m_stack.push(data);
        }
        void popImpl()
        {
            std::cout << "pop\n";
            m_stack.pop();
        }
        T& topImpl()
        {
            std::cout << "top\n";
            return m_stack.top();
        }
        bool isEmptyImpl()
        {
            std::cout << "empty\n";
            return m_stack.empty();
        }

        std::stack<T> m_stack;
    };
} // rma_treiber_stack


namespace stack_interface
{
    template<typename T>
    class IStack_traits<rma_treiber_stack::RmaTreiberStack < T>>
    {
        friend class IStack<rma_treiber_stack::RmaTreiberStack<T>>;
        friend class rma_treiber_stack::RmaTreiberStack<T>;
        typedef T ValueType;

    private:
        static void pushImpl(rma_treiber_stack::RmaTreiberStack<T>& stack, T& value)
        {
            stack.pushImpl(value);
        }
        static void popImpl(rma_treiber_stack::RmaTreiberStack<T>& stack)
        {
            stack.popImpl();
        }
        static ValueType& topImpl(rma_treiber_stack::RmaTreiberStack<T>& stack)
        {
            return stack.topImpl();
        }
        static bool isEmptyImpl(rma_treiber_stack::RmaTreiberStack<T>& stack)
        {
            return stack.isEmptyImpl();
        }
    };
}
#endif //SOURCES_RMATREIBERSTACK_HPP
