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
        explicit CountedNode(MPI_Aint t_address);
        explicit CountedNode();

        [[nodiscard]] MPI_Aint getAddress() const;

        [[nodiscard]] int getExternalCounter() const;
        void setAddress(MPI_Aint t_address);

        void incExternalCounter();
    private:
        int m_externalCounter;
        MPI_Aint m_address;
    };
} // rma_stack

#endif //SOURCES_COUNTEDNODE_H
