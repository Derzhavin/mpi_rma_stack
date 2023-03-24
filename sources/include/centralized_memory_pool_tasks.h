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
    auto pMainLogger = spdlog::get(mainLoggerName.data());

    MPI_Aint address{(MPI_Aint) MPI_BOTTOM};
    std::string msg = "address - " + std::to_string(address);
    SPDLOG_LOGGER_DEBUG(pMainLogger, msg);

    for (int i = 0; i < 1; ++i)
    {
        address = memoryPool.allocate();
        msg = "address - " + std::to_string(address);
        SPDLOG_LOGGER_DEBUG(pMainLogger, msg);
    }
}
#endif //SOURCES_CENTRALIZED_MEMORY_POOL_TASKS_H
