//
// Created by denis on 20.04.23.
//

#include <cmath>

#include "inner/Node.h"

namespace rma_stack::ref_counting
{
    Node::Node()
    :
    m_offset(0),
    m_rank(NodeDummyRank),
    m_internalCounter(0)
    {

    }

    int64_t Node::getInternalCounter() const
    {
        return m_internalCounter;
    }

    int64_t Node::getOffset() const
    {
        return m_offset;
    }

    uint64_t Node::getRank() const
    {
        return m_rank;
    }

    bool Node::incInternalCounter()
    {
        if (m_internalCounter + 1 > CountedNodeUpLimit)
            return false;

        ++m_internalCounter;
        return true;
    }

    bool Node::decInternalCounter()
    {
        if (std::abs(m_internalCounter - 1) > CountedNodeUpLimit)
            return false;
        --m_internalCounter;
        return true;
    }

    bool Node::isDummy() const
    {
        return std::abs(m_internalCounter) > NodeUpLimit;
    }
} // rma_stack