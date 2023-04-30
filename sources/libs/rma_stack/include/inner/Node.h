//
// Created by denis on 20.04.23.
//

#ifndef SOURCES_NODE_H
#define SOURCES_NODE_H

#include "ref_counting.h"
#include "CountedNodePtr.h"

#include <cstdint>

namespace rma_stack::ref_counting
{
    class Node
    {
    public:
        Node();
        [[nodiscard]] int64_t getInternalCounter() const;
        [[nodiscard]] uint64_t getOffset() const;
        [[nodiscard]] uint64_t getRank() const;
        bool incInternalCounter();
        bool decInternalCounter();
        [[nodiscard]] bool isDummy() const;
        [[nodiscard]] const CountedNodePtr &getCountedNodePtr() const;

    private:
        // First 8 bytes.
        uint64_t m_dataOffset   : OffsetBitsLimit;
        uint64_t m_dataRank     : RankBitsLimit;
        uint64_t                : 64 - OffsetBitsLimit - RankBitsLimit;

        // Second 8 bytes.
        uint32_t            : 32 - 1;
        uint32_t m_acquired : 1;
        int32_t m_internalCounter;

        // Third 8 bytes.
        CountedNodePtr m_countedNodePtr;
    };
} // rma_stack

#endif //SOURCES_NODE_H
