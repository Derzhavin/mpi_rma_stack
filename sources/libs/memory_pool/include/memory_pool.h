//
// Created by denis on 18.03.23.
//

#ifndef SOURCES_MEMORY_POOL_H
#define SOURCES_MEMORY_POOL_H

#include <mpi.h>

enum MemoryUsageMark: int
{
    Released = 1,
    Acquired = 2
};

#endif //SOURCES_MEMORY_POOL_H
