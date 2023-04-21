//
// Created by denis on 20.04.23.
//

#include "libs/rma_stack/include/inner/Node.h"

namespace rma_stack::ref_counting
{
    Node::Node()
    :
    m_offset(0),
    m_rank(NodeUpLimit),
    m_internalCounter(0),
    m_bInternalCounterNeg(0)
    {

    }

    int64_t Node::getInternalCounter() const {
        if (m_bInternalCounterNeg)
        {
            return -m_internalCounter;
        }
        return m_internalCounter;
    }

    int64_t Node::getOffset() const {
        return m_offset;
    }

    uint64_t Node::getRank() const {
        return m_rank;
    }

    bool Node::incInternalCounter() {
        if (m_bInternalCounterNeg)
        {
            --m_internalCounter;
            if (m_internalCounter == 0)
            {
                m_bInternalCounterNeg = 0;
            }
        }
        else
        {
            if (m_internalCounter + 1 > CountedNodeUpLimit)
                return false;

            ++m_internalCounter;
        }
        return true;
    }

    bool Node::decInternalCounter() {
        if (m_bInternalCounterNeg)
        {
            if (m_internalCounter + 1 > CountedNodeUpLimit)
                return false;

            ++m_internalCounter;
        }
        else if (m_internalCounter == 0)
        {
            m_internalCounter = 1;
            m_bInternalCounterNeg = 1;
        }
        else
        {
            --m_internalCounter;
        }
        return true;
    }

    bool Node::isDummy() const {
        return m_internalCounter > NodeUpLimit;
    }
} // rma_stack