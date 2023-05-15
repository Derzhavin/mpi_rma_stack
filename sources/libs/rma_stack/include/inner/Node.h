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
        [[nodiscard]] const CountedNodePtr &getCountedNodePtr() const;
        void setCountedNodePtrNext(const CountedNodePtr &t_countedNodePtr);

    private:
        // First 8 bytes.
        uint32_t m_acquired : 1;
        uint32_t m_reserved : 32 - 1;
        int32_t m_internalCounter;

        // Second 8 bytes.
        CountedNodePtr m_countedNodePtrNext;
    };
} // rma_stack

#endif //SOURCES_NODE_H
