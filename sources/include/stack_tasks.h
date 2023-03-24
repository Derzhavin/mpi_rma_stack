//
// Created by denis on 02.02.23.
//

#include "IStack.h"

template<typename StackImpl>
void runSimplePushPopTask(stack_interface::IStack<StackImpl>& stack)
{
    int i = 3;
    stack.push(i);
//    const auto v = stack.pop();
    auto stackSize = stack.size();
    std::cout << stackSize << '\n';
}