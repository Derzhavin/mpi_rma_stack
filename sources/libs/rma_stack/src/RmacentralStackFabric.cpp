//
// Created by denis on 04.04.23.
//

#include "RmaCentralStackFabric.h"

namespace rma_stack
{
    RmaCentralStackFabric &RmaCentralStackFabric::getInstance()
    {
        static RmaCentralStackFabric singleton;
        return singleton;
    }

    void RmaCentralStackFabric::registerCountedNodeMpiDatatype()
    {
        MPI_Datatype mpiDatatypeFields[2] = { MPI_AINT, MPI_INT };
        int blockLengths[2] = {1, 1};
        MPI_Aint offsets[2];
        const int fieldsNumber = 2;

        offsets[0] = offsetof(CountedNode_t, address);
        offsets[1] = offsetof(CountedNode_t, externalCounter);

        MPI_Type_create_struct(fieldsNumber, blockLengths, offsets, mpiDatatypeFields, &m_countedNodeMpiDatatype);
        MPI_Type_commit(&m_countedNodeMpiDatatype);
    }

    void RmaCentralStackFabric::registerCentralNodeMpiDatatype()
    {
        if (!isCountedNodeMpiDatatypeRegistered())
        {
            registerCountedNodeMpiDatatype();
        }

        MPI_Datatype mpiDatatypeFields[2] = {MPI_AINT, m_countedNodeMpiDatatype};
        int blockLengths[2] = {1, 1};
        MPI_Aint offsets[2];
        const int fieldsNumber = 2;

        offsets[0] = offsetof(CentralNode_t, dataAddress);
        offsets[1] = offsetof(CentralNode_t, countedNode);

        MPI_Type_create_struct(fieldsNumber, blockLengths, offsets, mpiDatatypeFields, &m_centralNodeMpiDatatype);
        MPI_Type_commit(&m_centralNodeMpiDatatype);
    }

    bool RmaCentralStackFabric::isCountedNodeMpiDatatypeRegistered() const
    {
        return m_countedNodeMpiDatatype != MPI_DATATYPE_NULL;
    }

    bool RmaCentralStackFabric::isCentralNodeMpiDatatypeRegistered() const
    {
        return m_centralNodeMpiDatatype != MPI_DATATYPE_NULL;
    }

    RmaCentralStackFabric::RmaCentralStackFabric()
    {
        registerCountedNodeMpiDatatype();
        registerCentralNodeMpiDatatype();
    }

    void RmaCentralStackFabric::finalize()
    {
        if (isCentralNodeMpiDatatypeRegistered())
        {
            releaseCentralNodeMpiDatatype();
        }
        if (isCountedNodeMpiDatatypeRegistered())
        {
            releaseCountedNodeMpiDatatype();
        }
    }

    void RmaCentralStackFabric::releaseCountedNodeMpiDatatype()
    {
        MPI_Type_free(&m_countedNodeMpiDatatype);
    }

    void RmaCentralStackFabric::releaseCentralNodeMpiDatatype()
    {
        MPI_Type_free(&m_centralNodeMpiDatatype);
    }
}