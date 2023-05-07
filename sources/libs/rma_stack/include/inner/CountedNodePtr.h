//
// Created by denis on 20.04.23.
//

#ifndef SOURCES_COUNTEDNODEPTR_H
#define SOURCES_COUNTEDNODEPTR_H

#include "ref_counting.h"

#include <cstdint>

namespace rma_stack::ref_counting
{
    class CountedNodePtr
    {
    public:
        CountedNodePtr();

        [[nodiscard]] int64_t getExternalCounter() const;
        bool setExternalCounter(int64_t t_externalCounter);
        [[nodiscard]] int64_t getOffset() const;
        bool setOffset(uint64_t t_offset);
        bool setRank(uint64_t t_rank);
        [[nodiscard]] uint64_t getRank() const;
        bool incExternalCounter();
        bool decExternalCounter();
        [[nodiscard]] bool isDummy() const;
    private:
        uint64_t m_offset               : OffsetBitsLimit;
        uint64_t m_rank                 : RankBitsLimit;
        uint64_t m_bExternalCounterNeg  : 1 ;
        uint64_t m_externalCounter      : ExternalCounterBitsLimit;

        friend bool operator==(CountedNodePtr& lhs, CountedNodePtr& rhs);
        friend bool operator!=(CountedNodePtr& lhs, CountedNodePtr& rhs);
    };

} // rma_stack

#endif //SOURCES_COUNTEDNODEPTR_H
