//
// Created by denis on 20.04.23.
//

#include <cmath>

#include "inner/Node.h"
#include "inner/CountedNodePtr.h"

namespace rma_stack::ref_counting
{
    Node::Node()
    :
    m_acquired(0),
    m_reserved(0),
    m_internalCounter(0)
    {

    }

    const rma_stack::ref_counting::CountedNodePtr &Node::getCountedNodePtr() const
    {
        return m_countedNodePtr;
    }
} // rma_stack