//
// Created by denis on 20.04.23.
//

#include <mpi.h>

#include "inner/CountedNodePtr.h"

namespace rma_stack::ref_counting
{
    int64_t CountedNodePtr::getExternalCounter() const
    {
        if (m_bExternalCounterNeg)
        {
            return -m_externalCounter;
        }
        return static_cast<int64_t>(m_externalCounter);
    }

    uint64_t CountedNodePtr::getRank() const
    {
        return m_rank;
    }

    bool CountedNodePtr::isDummy() const
    {
        return m_rank > CountedNodeUpLimit;
    }

    bool CountedNodePtr::incExternalCounter()
    {
        if (m_bExternalCounterNeg)
        {
            --m_externalCounter;
            if (m_externalCounter == 0)
            {
                m_bExternalCounterNeg = 0;
            }
        }
        else
        {
            if (m_externalCounter + 1 > CountedNodeUpLimit)
                return false;

            ++m_externalCounter;
        }
        return true;
    }

    bool CountedNodePtr::decExternalCounter()
    {
        if (m_bExternalCounterNeg)
        {
            if (m_externalCounter + 1 > CountedNodeUpLimit)
                return false;

            ++m_externalCounter;
        }
        else if (m_externalCounter == 0)
        {
            m_externalCounter = 1;
            m_bExternalCounterNeg = 1;
        }
        else
        {
            --m_externalCounter;
        }
        return true;
    }

    CountedNodePtr::CountedNodePtr():
    m_externalCounter(0),
    m_bExternalCounterNeg(0),
    m_rank(CountedNodePtrDummyRank),
    m_offset(0)
    {

    }

    int64_t CountedNodePtr::getOffset() const
    {
        return m_offset;
    }

    bool CountedNodePtr::setRank(uint64_t t_rank)
    {
        if (t_rank > CountedNodeUpLimit)
            return false;
        m_rank = t_rank;
        return true;
    }

    bool CountedNodePtr::setOffset(uint64_t t_offset) {
        if (t_offset < (1ul << OffsetBitsLimit))
            return false;

        m_offset = t_offset;
        return true;
    }
} // rma_stack