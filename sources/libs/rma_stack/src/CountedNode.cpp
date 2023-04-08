//
// Created by denis on 19.02.23.
//

#include "include/CountedNode.h"

namespace rma_stack
{
    CountedNode::CountedNode(MPI_Aint t_address)
    :
            m_address(t_address),
            m_externalCounter(0)
    {

    }

    CountedNode::CountedNode(): CountedNode((MPI_Aint)MPI_BOTTOM)
    {

    }

    void CountedNode::incExternalCounter()
    {
        m_externalCounter += 1;
    }

    MPI_Aint CountedNode::getAddress() const
    {
        return m_address;
    }

    void CountedNode::setAddress(MPI_Aint t_address)
    {
        CountedNode::m_address = t_address;
    }

    int CountedNode::getExternalCounter() const
    {
        return m_externalCounter;
    }
} // rma_stack