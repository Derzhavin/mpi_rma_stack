//
// Created by denis on 19.02.23.
//

#ifndef SOURCES_COUNTEDNODE_H
#define SOURCES_COUNTEDNODE_H

#include <mpi.h>

namespace rma_stack
{
    class CountedNode
    {
    public:
        explicit CountedNode(MPI_Aint t_nodeDisp);
        explicit CountedNode();

        int externalCounter;
        MPI_Aint nodeDisp;
    };
} // rma_stack

#endif //SOURCES_COUNTEDNODE_H
