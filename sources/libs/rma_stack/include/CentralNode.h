//
// Created by denis on 18.02.23.
//

#ifndef SOURCES_CENTRALNODE_H
#define SOURCES_CENTRALNODE_H

#include <mpi.h>

#include "CountedNode.h"

namespace rma_stack
{
    typedef struct
    {
        MPI_Aint dataAddress;
        CountedNode_t countedNode;
    } CentralNode_t;
} // rma_stack

#endif //SOURCES_CENTRALNODE_H
