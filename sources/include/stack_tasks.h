//
// Created by denis on 02.02.23.
//

#include <iostream>
#include <type_traits>

#include "IStack.h"
#include "inner/InnerStack.h"

template<typename StackImpl>
using EnableIfValueTypeIsInt = std::enable_if_t<std::is_same_v<typename StackImpl::ValueType, int>>;

template<typename StackImpl,
        typename = EnableIfValueTypeIsInt<StackImpl>>
void runStackSimpleIntPushPopTask(stack_interface::IStack<StackImpl> &stack, MPI_Comm comm)
{
    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    int size{0};
    MPI_Comm_size(comm, &size);

    int elems[5]{0};

    for (int i = 0; i < sizeof(elems); ++i)
    {
        elems[i] = i * size + rank;
    }

    for (auto& elem: elems)
    {
        stack.push(elem);
        SPDLOG_DEBUG("received elem by 'push' {}", elem);
    }

    for (int i = 0; i < sizeof(elems); ++i)
    {
        int elem = stack.pop();
        SPDLOG_DEBUG("brought back elem by 'popHalf' {}", elem);
    }
}

void runInnerStackSimplePushPopTask(rma_stack::ref_counting::InnerStack &stack, int comm);