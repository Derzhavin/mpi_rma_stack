//
// Created by denis on 25.04.23.
//

#include "include/stack_tasks.h"

void runInnerStackSimplePushPopTask(rma_stack::ref_counting::InnerStack &stack, int comm)
{
    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    rma_stack::ref_counting::GlobalAddress pushedAddresses[5]{0};

    for (auto& dataAddress: pushedAddresses)
    {
        dataAddress = stack.push(rank);
        SPDLOG_DEBUG("received address by 'push' {}", address);
    }

    for (int i = 0; i < sizeof(pushedAddresses); ++i)
    {
        auto address = stack.popHalf();
        SPDLOG_DEBUG("brought back address by 'popHalf' {}", address);
    }
}