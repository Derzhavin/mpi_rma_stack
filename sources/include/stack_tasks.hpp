//
// Created by denis on 02.02.23.
//

#include "IStack.hpp"

template<typename StackImpl>
void runSimplePushPopTask(stack_interface::IStack<StackImpl>& stack)
{
    int i = 3;
    stack.push(i);
    stack.pop();
}