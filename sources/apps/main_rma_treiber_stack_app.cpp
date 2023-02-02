//
// Created by denis on 22.01.23.
//

#include "RmaTreiberStack.hpp"

int main(int argc, char *argv[])
{
    rma_treiber_stack::RmaTreiberStack<int> rmaTreiberStack;
    int i = 3;
    rmaTreiberStack.push(i);
    std::cout << rmaTreiberStack.top() << '\n';
    rmaTreiberStack.pop();
    std::cout << rmaTreiberStack.isEmpty() << '\n';

    return 0;
}