//
// Created by denis on 19.02.23.
//

#ifndef SOURCES_COUNTEDNODE_H
#define SOURCES_COUNTEDNODE_H

#include <mpi.h>

namespace rma_stack
{
    typedef struct
    {
        MPI_Aint address;
        int externalCounter;
    } CountedNode_t;

} // rma_stack

#endif //SOURCES_COUNTEDNODE_H
