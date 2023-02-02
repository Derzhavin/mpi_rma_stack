//
// Created by denis on 22.01.23.
//

#include "RmaTreiberStack.hpp"

int main(int argc, char *argv[])
{
    MPI_Comm comm;
    rma_treiber_stack::RmaTreiberStack<int> rmaTreiberStack(comm);
    int i = 3;
    rmaTreiberStack.push(i);
    rmaTreiberStack.pop();

    return 0;
}