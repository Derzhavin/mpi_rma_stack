//
// Created by denis on 25.04.23.
//

#include "include/stack_tasks.h"

void runInnerStackSimplePushPopTask(rma_stack::ref_counting::InnerStack &stack, int comm)
{
    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    rma_stack::ref_counting::DataAddress pushedAddresses[5]{0};

    for (auto& dataAddress: pushedAddresses)
    {
        dataAddress = stack.allocatePush(rank);
        SPDLOG_DEBUG("received address by 'allocatePush' {}", address);
    }

    for (int i = 0; i < sizeof(pushedAddresses); ++i)
    {
        auto address = stack.deallocatePop();
        SPDLOG_DEBUG("brought back address by 'deallocatePop' {}", address);
    }
}