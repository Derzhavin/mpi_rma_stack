//
// Created by denis on 18.03.23.
//

#ifndef SOURCES_CENTRALIZED_MEMORY_POOL_TASKS_H
#define SOURCES_CENTRALIZED_MEMORY_POOL_TASKS_H

#include "include/logging.h"

#include "CentralizedMemoryPool.h"

template<typename T>
void simpleMemoryAllocateAndDeallocateTask(CentralizedMemoryPool<T> &memoryPool, MPI_Comm comm, int rank)
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
#endif //SOURCES_CENTRALIZED_MEMORY_POOL_TASKS_H
