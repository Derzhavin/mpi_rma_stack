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
    constexpr uint64_t DummyRank                = (1 << RankBitsLimit) - 1;

    struct GlobalAddress
    {
        uint64_t offset   : OffsetBitsLimit;
        uint64_t rank     : RankBitsLimit;
        uint64_t reserved   : 64 - OffsetBitsLimit - RankBitsLimit;
    };

    bool isGlobalAddressDummy(GlobalAddress globalAddress);
    bool isValidRank(uint64_t rank);
}
#endif //SOURCES_REF_COUNTING_H
