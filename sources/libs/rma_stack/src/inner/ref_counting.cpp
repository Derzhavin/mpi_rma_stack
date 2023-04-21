//
// Created by denis on 20.04.23.
//

#include "inner/ref_counting.h"

bool rma_stack::ref_counting::isDummyAddress(DataAddress dataAddress)
{
    return dataAddress.rank == CountedNodePtrDummyRank;
}

