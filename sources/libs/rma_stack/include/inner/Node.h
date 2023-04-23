//
// Created by denis on 20.04.23.
//

#ifndef SOURCES_NODE_H
#define SOURCES_NODE_H

#include "ref_counting.h"

#include <cstdint>

namespace rma_stack::ref_counting
{
    class Node
    {
    public:
        Node();
        [[nodiscard]] int64_t getInternalCounter() const;
        [[nodiscard]] int64_t getOffset() const;
        [[nodiscard]] uint64_t getRank() const;
        bool incInternalCounter();
        bool decInternalCounter();
        [[nodiscard]] bool isDummy() const;

    private:
        uint64_t m_offset               : OffsetBitsLimit;
        uint64_t m_rank                 : RankBitsLimit;
        uint64_t reserved              : 64 - OffsetBitsLimit - RankBitsLimit;
        int64_t m_internalCounter;
    };
} // rma_stack

#endif //SOURCES_NODE_H
