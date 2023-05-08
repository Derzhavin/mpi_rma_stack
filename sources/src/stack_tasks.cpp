//
// Created by denis on 25.04.23.
//

#include "include/stack_tasks.h"

void runInnerStackSimplePushPopTask(rma_stack::ref_counting::InnerStack &stack, int comm)
{
    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    const int pushedAddressesSize{5};
    rma_stack::ref_counting::GlobalAddress pushedAddresses[pushedAddressesSize];

    for (auto& dataAddress: pushedAddresses)
    {
        stack.push([&dataAddress](const rma_stack::ref_counting::GlobalAddress &t_dataAddress) {
            dataAddress = t_dataAddress;
        });
        SPDLOG_DEBUG("received address by 'push' {}", dataAddress);
    }

    for (int i = 0; i < pushedAddressesSize; ++i)
    {
        rma_stack::ref_counting::GlobalAddress dataAddress{0, rma_stack::ref_counting::DummyRank, 0};

        stack.pop([&dataAddress] (const rma_stack::ref_counting::GlobalAddress& t_dataAddress){
            dataAddress = t_dataAddress;
        });
        SPDLOG_DEBUG("brought back address by 'pop' {}", dataAddress);
    }
}