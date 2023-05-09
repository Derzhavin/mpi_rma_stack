//
// Created by denis on 25.04.23.
//

#include "include/stack_tasks.h"

void runInnerStackSimplePushPopTask(rma_stack::ref_counting::InnerStack &stack, MPI_Comm comm)
{
    spdlog::debug("started 'runInnerStackSimplePushPopTask'");
    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    const int pushedAddressesSize{5};
    rma_stack::ref_counting::GlobalAddress pushedAddresses[pushedAddressesSize];

    for (auto& dataAddress: pushedAddresses)
    {
        stack.push([&dataAddress](const rma_stack::ref_counting::GlobalAddress &t_dataAddress) {
            dataAddress = t_dataAddress;
        });
        const auto r = dataAddress.rank;
        const auto o = dataAddress.offset;
        spdlog::debug("received address by 'push' ({}, {})", r, o);
    }

    for (int i = 0; i < pushedAddressesSize; ++i)
    {
        rma_stack::ref_counting::GlobalAddress dataAddress{0, rma_stack::ref_counting::DummyRank, 0};

        stack.pop([&dataAddress] (const rma_stack::ref_counting::GlobalAddress& t_dataAddress){
            dataAddress = t_dataAddress;
        });
        const auto r = dataAddress.rank;
        const auto o = dataAddress.offset;
        spdlog::debug("received address by 'pop' ({}, {})", r, o);
    }
}