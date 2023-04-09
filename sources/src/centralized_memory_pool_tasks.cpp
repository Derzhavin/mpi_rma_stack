//
// Created by denis on 09.04.23.
//

#include "include/centralized_memory_pool_tasks.h"

void simpleMemoryAllocateAndDeallocateTask(CentralizedMemoryPool &memoryPool, MPI_Comm comm, int rank)
{
    constexpr size_t addressesNum = 3;
    MPI_Aint addresses[addressesNum]{(MPI_Aint)MPI_BOTTOM};

    for (auto& address: addresses)
    {
        address = memoryPool.allocate();
        SPDLOG_DEBUG("allocated memory at the m_address {}", address);
    }
    for (auto& address: addresses)
    {
        memoryPool.deallocate(address);
        SPDLOG_DEBUG("deallocated memory at the m_address {}", address);
    }
}