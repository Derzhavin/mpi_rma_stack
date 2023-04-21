//
// Created by denis on 20.04.23.
//

#ifndef SOURCES_REF_COUNTING_H
#define SOURCES_REF_COUNTING_H

#include <cstdint>

namespace rma_stack::ref_counting
{
    constexpr uint64_t OffsetBitsLimit          = 39;
    constexpr uint64_t RankBitsLimit            = 12;
    constexpr uint64_t ExternalCounterBitsLimit = RankBitsLimit;
    constexpr uint64_t InternalCounterBitsLimit = RankBitsLimit;
    constexpr uint64_t CountedNodeUpLimit       = (1 << RankBitsLimit) - 1;
    constexpr uint64_t CountedNodePtrDummyRank     = (1 << RankBitsLimit) - 1;
    constexpr uint64_t NodeUpLimit              = (1 << RankBitsLimit) - 1;
    constexpr uint64_t NodeDummyRank            = (1 << RankBitsLimit) - 1;

    struct DataAddress
    {
        uint64_t offset   : OffsetBitsLimit;
        uint64_t rank     : RankBitsLimit;
        uint64_t reserved   : 64 - OffsetBitsLimit - RankBitsLimit;
    };

    bool isDummyAddress(DataAddress dataAddress);
}
#endif //SOURCES_REF_COUNTING_H
