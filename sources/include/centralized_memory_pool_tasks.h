//
// Created by denis on 18.03.23.
//

#ifndef SOURCES_CENTRALIZED_MEMORY_POOL_TASKS_H
#define SOURCES_CENTRALIZED_MEMORY_POOL_TASKS_H

#include "include/logging.h"

#include "CentralizedMemoryPool.h"

void simpleMemoryAllocateAndDeallocateTask(CentralizedMemoryPool &memoryPool, MPI_Comm comm, int rank);

#endif //SOURCES_CENTRALIZED_MEMORY_POOL_TASKS_H
