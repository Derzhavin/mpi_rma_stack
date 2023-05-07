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
    m_acquired(1),
    m_internalCounter(0)
    {

    }

    int64_t Node::getInternalCounter() const
    {
        return m_internalCounter;
    }

    bool Node::incInternalCounter()
    {
        if (m_internalCounter + 1 > DummyRank)
            return false;

        ++m_internalCounter;
        return true;
    }

    bool Node::decInternalCounter()
    {
        if (std::abs(m_internalCounter - 1) > DummyRank)
            return false;
        --m_internalCounter;
        return true;
    }

    const rma_stack::ref_counting::CountedNodePtr &Node::getCountedNodePtr() const
    {
        return m_countedNodePtr;
    }
} // rma_stack