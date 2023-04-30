//
// Created by denis on 20.04.23.
//

#include "inner/InnerStack.h"
#include "MpiException.h"

namespace rma_stack::ref_counting
{
    namespace custom_mpi = custom_mpi_extensions;

    void InnerStack::push(GlobalAddress nodeAddress)
    {
        CountedNodePtr newCountedNodePtr;
        newCountedNodePtr.setRank(nodeAddress.rank);
        newCountedNodePtr.setOffset(nodeAddress.offset);
        newCountedNodePtr.incExternalCounter();

        CountedNodePtr resCountedNodePtr;

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started new head pushing in 'push'",
                            m_rank
        );

        MPI_Win_lock_all(0, m_headWin);

        MPI_Fetch_and_op(&resCountedNodePtr,
                         &resCountedNodePtr,
                         MPI_UINT64_T,
                         CENTRAL_RANK,
                         0,
                         MPI_NO_OP,
                         m_headWin
        );
        MPI_Win_flush(CENTRAL_RANK, m_headWin);

        do
        {
            *m_pHeadCountedNodePtr = resCountedNodePtr;
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 m_pHeadCountedNodePtr,
                                 &resCountedNodePtr,
                                 MPI_UINT64_T,
                                 CENTRAL_RANK,
                                 0,
                                 m_headWin
            );
            MPI_Win_flush(CENTRAL_RANK, m_headWin);
            SPDLOG_LOGGER_TRACE(m_logger,
                                "rank %d executed CAS in 'push'",
                                m_rank
            );
        }
        while (resCountedNodePtr != *m_pHeadCountedNodePtr);

        MPI_Win_unlock_all(m_headWin);

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'push'",
                            m_rank
        );
    }

    GlobalAddress InnerStack::acquireNode(int rank) const
    {
        GlobalAddress nodeGlobalAddress = { .rank = DummyRank};

        if (!isValidRank(rank))
            return nodeGlobalAddress;

        uint32_t newAcquiredField{1};
        uint32_t oldAcquiredField{0};
        uint32_t resAcquiredField{0};

        for (MPI_Aint disp = 0; disp < m_elemsUpLimit; ++disp)
        {
            MPI_Aint nodeOffset = MPI_Aint_add(m_nodeArrBase, disp * sizeof(Node));
            MPI_Aint acquiredFieldOffset = nodeOffset + 64;

            MPI_Win_lock_all(0, m_nodeWin);
            MPI_Compare_and_swap(&newAcquiredField,
                                 &oldAcquiredField,
                                 &resAcquiredField,
                                 MPI_UINT32_T,
                                 rank,
                                 acquiredFieldOffset,
                                 m_nodeWin
            );
            MPI_Win_flush_all(m_nodeWin);
            MPI_Win_unlock_all(m_nodeWin);

            assert(resAcquiredField == 0 || resAcquiredField == 1);
            if (resAcquiredField)
            {
                nodeGlobalAddress.rank = rank;
                nodeGlobalAddress.offset = nodeOffset;
                break;
            }
        }
        return nodeGlobalAddress;
    }

    void InnerStack::releaseNode(GlobalAddress nodeAddress) const
    {
        MPI_Aint acquiredFieldOffset = nodeAddress.offset + 64;

        GlobalAddress acquiredField{1};

        MPI_Win_lock_all(0, m_nodeWin);
        MPI_Fetch_and_op(&acquiredField,
                         &acquiredField,
                         MPI_UINT32_T,
                         nodeAddress.rank,
                         acquiredFieldOffset,
                         MPI_REPLACE,
                         m_nodeWin
        );
        MPI_Win_flush(nodeAddress.rank, m_headWin);
        MPI_Win_unlock_all(m_nodeWin);
    }

    GlobalAddress InnerStack::popHalf()
    {
        MPI_Win_lock_all(0, m_headWin);

        MPI_Fetch_and_op(&m_pHeadCountedNodePtr,
                         &m_pHeadCountedNodePtr,
                         MPI_UINT64_T,
                         CENTRAL_RANK,
                         0,
                         MPI_NO_OP,
                         m_headWin
        );
        MPI_Win_flush(CENTRAL_RANK, m_headWin);
        MPI_Win_unlock_all(m_headWin);

        CountedNodePtr oldHeadCountedNodePtr = *m_pHeadCountedNodePtr;

        for (;;)
        {
            increaseHeadCount(oldHeadCountedNodePtr);
            GlobalAddress nodeAddress = {
                    .offset = static_cast<uint64_t>(oldHeadCountedNodePtr.getOffset()),
                    .rank = oldHeadCountedNodePtr.getRank()
            };
            if (isGlobalAddressDummy(nodeAddress))
                return nodeAddress;

            Node node;
            constexpr auto nodeSize = sizeof(Node);
            MPI_Win_lock_all(0, m_nodeWin);
            MPI_Get(&node,
                    nodeSize,
                    MPI_UNSIGNED_CHAR,
                    nodeAddress.rank,
                    nodeAddress.offset,
                    nodeSize,
                    MPI_UNSIGNED_CHAR,
                    m_nodeWin
            );
            MPI_Win_unlock_all(m_nodeWin);

            auto& newCountedNodePtr = node.getCountedNodePtr();
            CountedNodePtr resCountedNodePtr;

            MPI_Win_lock_all(0, m_headWin);
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 &oldHeadCountedNodePtr,
                                 &resCountedNodePtr,
                                 MPI_UINT64_T,
                                 m_headAddress.rank,
                                 m_headAddress.offset,
                                 m_headWin
            );
            MPI_Win_flush_all(m_headWin);
            MPI_Win_unlock_all(m_headWin);

            if (resCountedNodePtr == oldHeadCountedNodePtr)
            {
                GlobalAddress internalCounterAddress = {
                        .offset = nodeAddress.offset + 3 * 4,
                        .rank = nodeAddress.rank
                };
                auto const externalCount = static_cast<int32_t>(oldHeadCountedNodePtr.getExternalCounter());
                int32_t const countIncrease = externalCount - 2;
                int32_t resInternalCount{0};
                MPI_Fetch_and_op(
                        &countIncrease,
                        &resInternalCount,
                        MPI_INT64_T,
                        internalCounterAddress.rank,
                        internalCounterAddress.offset,
                        MPI_SUM,
                        m_nodeWin
                );

                if (resInternalCount == -countIncrease)
                {

                }

                return nodeAddress;
            }
            GlobalAddress nodeAddress_ = {
                    .offset = static_cast<uint64_t>(oldHeadCountedNodePtr.getOffset()),
                    .rank = oldHeadCountedNodePtr.getRank()
            };

            GlobalAddress internalCounterAddress = {
                    .offset = nodeAddress_.m_offset + 1,
                    .rank = nodeAddress_.m_rank
            };
            int64_t const countIncrease{-1};
            int64_t resultInternalCount{0};
            MPI_Fetch_and_op(
                    &countIncrease,
                    &resultInternalCount,
                    MPI_INT64_T,
                    internalCounterAddress.m_rank,
                    internalCounterAddress.m_offset,
                    MPI_SUM,
                    m_nodeWin
            );

            if (resultInternalCount == 1)
            {
                releaseNode(nodeAddress_);
            }
        }
    }

    void InnerStack::increaseHeadCount(CountedNodePtr &oldCountedNodePtr)
    {
        CountedNodePtr newCountedNodePtr;

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started 'increaseHeadCount'",
                            m_rank
        );

        MPI_Win_lock_all(0, m_headWin);

        do
        {
            newCountedNodePtr = oldCountedNodePtr;
            newCountedNodePtr.incExternalCounter();
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 m_pHeadCountedNodePtr,
                                 &oldCountedNodePtr,
                                 MPI_UINT64_T,
                                 CENTRAL_RANK,
                                 0,
                                 m_headWin
            );
            MPI_Win_flush(CENTRAL_RANK, m_headWin);
            SPDLOG_LOGGER_TRACE(m_logger,
                                "rank %d executed CAS in 'increaseHeadCount'",
                                m_rank
            );
        }
        while (oldCountedNodePtr != *m_pHeadCountedNodePtr);
        MPI_Win_unlock_all(m_headWin);

        oldCountedNodePtr.setExternalCounter(newCountedNodePtr.getExternalCounter());

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'increaseHeadCount'",
                            m_rank
        );
    }

    InnerStack::InnerStack(MPI_Comm comm, MPI_Info info, size_t t_elemsUpLimit,
                           std::shared_ptr<spdlog::logger> t_logger)
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
            auto mpiStatus = MPI_Win_create_dynamic(info, comm, &m_nodeWin);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to create RMA window for node array", __FILE__, __func__, __LINE__, mpiStatus);
        }
        {
            auto mpiStatus = MPI_Win_create_dynamic(info, comm, &m_headWin);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to create RMA window for head", __FILE__, __func__, __LINE__, mpiStatus);
        }
        allocateProprietaryData(comm);
        initStackWithDummy();
    }

    void InnerStack::release()
    {
        MPI_Free_mem(m_pNodeArr);
        m_pNodeArr = nullptr;
        SPDLOG_LOGGER_TRACE(m_logger, "freed up node arr RMA memory");

        MPI_Win_free(&m_nodeWin);
        SPDLOG_LOGGER_TRACE(m_logger, "freed up node win RMA memory");

        MPI_Free_mem(m_pHeadCountedNodePtr);
        m_pHeadCountedNodePtr = nullptr;
        SPDLOG_LOGGER_TRACE(m_logger, "freed up head pointer RMA memory");

        MPI_Win_free(&m_headWin);
        SPDLOG_LOGGER_TRACE(m_logger, "freed up head win RMA memory");
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
                auto mpiStatus = MPI_Win_attach(m_nodeWin, m_pNodeArr, m_elemsUpLimit);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
            }
            MPI_Get_address(m_pNodeArr, &m_nodeArrBase);

            {
                auto mpiStatus = MPI_Alloc_mem(sizeof(CountedNodePtr), MPI_INFO_NULL,
                                               &m_pHeadCountedNodePtr);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException(
                            "failed to allocate RMA memory",
                            __FILE__,
                            __func__,
                            __LINE__,
                            mpiStatus
                    );
            }
            *m_pHeadCountedNodePtr = CountedNodePtr();
            {
                auto mpiStatus = MPI_Win_attach(m_headWin, m_pHeadCountedNodePtr, 1);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
            }
            MPI_Aint address{(MPI_Aint(MPI_BOTTOM))};
            MPI_Get_address(m_pHeadCountedNodePtr, &address);
            m_headAddress.m_offset    = address;
            m_headAddress.m_rank      = 1;
        }

        {
            auto mpiStatus = MPI_Bcast(&m_nodeArrBase, 1, MPI_AINT, CENTRAL_RANK, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast base elements' memory usage m_address", __FILE__, __func__ , __LINE__, mpiStatus);
        }
        {
            auto mpiStatus = MPI_Bcast(&m_headAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast head address", __FILE__, __func__ , __LINE__, mpiStatus);
        }
    }

    void InnerStack::initStackWithDummy()
    {

        if (isCentralRank())
        {
            MPI_Win_lock_all(0, m_headWin);
            CountedNodePtr dummyCountedNodePtr;
            MPI_Put(&dummyCountedNodePtr,
                    1,
                    MPI_UINT64_T,
                    CENTRAL_RANK,
                    0,
                    1,
                    MPI_UINT64_T,
                    m_headWin
            );
            MPI_Win_flush(CENTRAL_RANK, m_headWin);
            MPI_Win_unlock_all(m_headWin);
        }
    }

    size_t InnerStack::getElemsUpLimit() const
    {
        return m_elemsUpLimit;
    }

    void InnerStack::putDataAddressInNode(const GlobalAddress &nodeAddress, const GlobalAddress &dataAddress) const
    {
        MPI_Win_lock_all(0, m_nodeWin);
        MPI_Put(&dataAddress,
                1,
                MPI_UINT64_T,
                nodeAddress.rank,
                nodeAddress.offset,
                1,
                MPI_UINT64_T,
                m_nodeWin
        );
        MPI_Win_flush(nodeAddress.rank, m_nodeWin);
        MPI_Win_unlock_all(m_nodeWin);
    }
} // ref_counting