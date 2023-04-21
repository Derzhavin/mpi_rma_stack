//
// Created by denis on 20.04.23.
//

#include "inner/InnerStack.h"
#include "MpiException.h"

namespace rma_stack::ref_counting
{
    namespace custom_mpi = custom_mpi_extensions;

    DataAddress InnerStack::allocatePush(uint64_t rank)
    {
        auto dataAddress = acquireFreeAddress(rank);

        if (isDummyAddress(dataAddress))
            return dataAddress;

        CountedNodePtr newNode;
        newNode.setRank(dataAddress.offset);
        newNode.setOffset(dataAddress.offset);

        newNode.incExternalCounter();

        return dataAddress;
    }

    DataAddress InnerStack::acquireFreeAddress(uint64_t rank) const
    {
        DataAddress dataAddress = {
                .rank = CountedNodePtrDummyRank
        };

        CountedNodePtr newNode;
        if (!newNode.setRank(rank))
        {
            return dataAddress;
        }

        CountedNodePtr oldNode;
        CountedNodePtr resNode;
        for (MPI_Aint i = 0; i < m_elemsUpLimit; ++i)
        {
            MPI_Win_lock_all(0, m_countedNodePtrMpiWin);
            MPI_Compare_and_swap(&newNode,
                                 &oldNode,
                                 &resNode,
                                 MPI_UINT64_T,
                                 CENTRAL_RANK,
                                 m_countedNodePtrArrBaseAddress,
                                 m_countedNodePtrMpiWin
            );
            MPI_Win_flush_all(m_countedNodePtrMpiWin);
            MPI_Win_unlock_all(m_countedNodePtrMpiWin);

            if (resNode.getRank() != CountedNodePtrDummyRank)
            {
                dataAddress = {
                        .offset = static_cast<uint64_t>(newNode.getOffset()),
                        .rank = newNode.getRank()
                };
                return dataAddress;
            }
        }
        return dataAddress;
    }

    DataAddress InnerStack::deallocatePop()
    {
        return DataAddress();
    }

    void InnerStack::increaseHeadCount(CountedNodePtr &oldCountedNode)
    {

    }

    InnerStack::InnerStack(
            MPI_Comm comm,
            MPI_Info info,
            size_t t_elemsUpLimit,
            std::shared_ptr<spdlog::logger> t_logger
    )
    :
    m_elemsUpLimit(t_elemsUpLimit),
    m_logger(std::move(t_logger))
    {
        {
            auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to get rank", __FILE__, __func__, __LINE__, mpiStatus);
        }
        {
            auto mpiStatus = MPI_Win_create_dynamic(info, comm, &m_countedNodePtrMpiWin);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to create RMA window for counted node ptr array", __FILE__, __func__, __LINE__, mpiStatus);
        }
        {
            auto mpiStatus = MPI_Win_create_dynamic(info, comm, &m_nodeMpiWin);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to create RMA window for node array", __FILE__, __func__, __LINE__, mpiStatus);
        }

        allocateProprietaryData(comm);


        MPI_Put(m_pCountedNodePtrArr,
                1,
                MPI_UINT64_T,
                CENTRAL_RANK,
                m_countedNodePtrArrBaseAddress,
                1,
                MPI_UINT64_T,
                m_countedNodePtrMpiWin
        );
    }

    void InnerStack::release()
    {
        MPI_Free_mem(m_pCountedNodePtrArr);
        m_pCountedNodePtrArr = nullptr;
        SPDLOG_LOGGER_TRACE(logger, "freed up counted node ptr arr RMA memory");

        MPI_Free_mem(m_pNodeArr);
        m_pNodeArr = nullptr;
        SPDLOG_LOGGER_TRACE(logger, "freed up node arr RMA memory");
    }

    bool InnerStack::isCentralRank() const
    {
        return m_rank == CENTRAL_RANK;;
    }

    void InnerStack::allocateProprietaryData(MPI_Comm comm)
    {
        if (isCentralRank())
        {
            {
                auto mpiStatus = MPI_Alloc_mem(sizeof(CountedNodePtr) * m_elemsUpLimit, MPI_INFO_NULL,
                                               &m_pCountedNodePtrArr);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException(
                            "failed to allocate RMA memory",
                            __FILE__,
                            __func__,
                            __LINE__,
                            mpiStatus
                    );
            }
            std::fill_n(m_pCountedNodePtrArr, m_elemsUpLimit, CountedNodePtr());
            {
                auto mpiStatus = MPI_Win_attach(m_countedNodePtrMpiWin, m_pCountedNodePtrArr, m_elemsUpLimit);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
            }
            MPI_Get_address(m_pCountedNodePtrArr, &m_countedNodePtrArrBaseAddress);

            {
                auto mpiStatus = MPI_Alloc_mem(sizeof(Node) * m_elemsUpLimit, MPI_INFO_NULL,
                                               &m_pNodeArr);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException(
                            "failed to allocate RMA memory",
                            __FILE__,
                            __func__,
                            __LINE__,
                            mpiStatus
                    );
            }
            std::fill_n(m_pNodeArr, m_elemsUpLimit, Node());
            {
                auto mpiStatus = MPI_Win_attach(m_nodeMpiWin, m_pNodeArr, m_elemsUpLimit);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
            }
            MPI_Get_address(m_pNodeArr, &m_nodeArrBaseAddress);
        }

        {
            auto mpiStatus = MPI_Bcast(&m_countedNodePtrArrBaseAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast element size", __FILE__, __func__ , __LINE__, mpiStatus);
        }
        {
            auto mpiStatus = MPI_Bcast(&m_nodeArrBaseAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast base elements' memory usage m_address", __FILE__, __func__ , __LINE__, mpiStatus);
        }
    }
} // ref_counting