//
// Created by denis on 04.04.23.
//

#include "RmaStackFabric.h"

namespace rma_stack
{
    bool RmaStackFabric::m_sCentralNodeMpiDatatypeRegistered = false;
    bool RmaStackFabric::m_sCountedNodeMpiDatatypeRegistered = false;

    MPI_Datatype RmaStackFabric::registerCountedNodeMpiDatatype() {
        return 0;
    }

    MPI_Datatype RmaStackFabric::registerCentralNodeMpiDatatype() {
        return 0;
    }

    MPI_Datatype registerCountedNodeMpiDatatype()
    {

    }
}