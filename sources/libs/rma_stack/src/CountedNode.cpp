//
// Created by denis on 19.02.23.
//

#include "include/CountedNode.h"

namespace rma_stack
{
    CountedNode::CountedNode(MPI_Aint t_nodeDisp)
    :
    nodeDisp(t_nodeDisp),
    externalCounter(0)
    {

    }

    CountedNode::CountedNode(): CountedNode((MPI_Aint)MPI_BOTTOM)
    {

    }
} // rma_stack