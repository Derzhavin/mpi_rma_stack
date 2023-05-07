//
// Created by denis on 20.04.23.
//

#include "inner/InnerStack.h"
#include "MpiException.h"

namespace rma_stack::ref_counting
{
    namespace custom_mpi = custom_mpi_extensions;

    bool InnerStack::push(const std::function<void(GlobalAddress)> &putDataCallback)
    {
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started 'tryPush'",
                            m_rank
        );

        auto nodeAddress = acquireNode(m_centralized ? m_headRank : m_rank);
        if (isGlobalAddressDummy(nodeAddress))
        {
            SPDLOG_LOGGER_TRACE(m_logger,
                                "rank %d failed to find free node in 'tryPush'",
                                m_rank
            );
            return false;
        }
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d acquired free node in 'tryPush'",
                            m_rank
        );

        putDataCallback(nodeAddress);
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d put data in 'tryPush'",
                            m_rank
        );

        CountedNodePtr newCountedNodePtr;
        newCountedNodePtr.setRank(nodeAddress.rank);
        newCountedNodePtr.setOffset(nodeAddress.offset);
        newCountedNodePtr.incExternalCounter();

        CountedNodePtr resCountedNodePtr;

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started new head pushing in 'push'",
                            m_rank
        );

        MPI_Win_lock(MPI_LOCK_SHARED, m_headRank, 0, m_headWin);

        {
            MPI_Fetch_and_op(&resCountedNodePtr,
                             &resCountedNodePtr,
                             MPI_UINT64_T,
                             m_headRank,
                             m_headAddress,
                             MPI_NO_OP,
                             m_headWin
            );
            MPI_Win_flush(m_headRank, m_headWin);
        }

        do
        {
            *m_pHeadCountedNodePtr = resCountedNodePtr;

            MPI_Compare_and_swap(&newCountedNodePtr,
                                 m_pHeadCountedNodePtr,
                                 &resCountedNodePtr,
                                 MPI_UINT64_T,
                                 m_headRank,
                                 m_headAddress,
                                 m_headWin
            );
            MPI_Win_flush(m_headRank, m_headWin);
            SPDLOG_LOGGER_TRACE(m_logger,
                                "rank %d executed CAS in 'push'",
                                m_rank
            );
        }
        while (resCountedNodePtr != *m_pHeadCountedNodePtr);

        MPI_Win_unlock(m_headRank, m_headWin);

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'push'",
                            m_rank
        );
        return true;
    }

    GlobalAddress InnerStack::acquireNode(int rank) const
    {
        GlobalAddress nodeGlobalAddress = { .rank = DummyRank};

        if (!isValidRank(rank))
            return nodeGlobalAddress;

        uint32_t newAcquiredField{1};
        uint32_t oldAcquiredField{0};
        uint32_t resAcquiredField{0};

        for (MPI_Aint i = 0; i < m_elemsUpLimit; ++i)
        {
            constexpr auto nodeSize = static_cast<MPI_Aint>(sizeof(Node));
            const auto nodeDisplacement = i * nodeSize;
            const MPI_Aint nodeOffset = MPI_Aint_add(m_pNodeArrAddresses[rank], nodeDisplacement);

            MPI_Win_lock(MPI_LOCK_SHARED, rank, 0, m_nodeWin);
            MPI_Compare_and_swap(&newAcquiredField,
                                 &oldAcquiredField,
                                 &resAcquiredField,
                                 MPI_UINT32_T,
                                 rank,
                                 nodeOffset,
                                 m_nodeWin
            );
            MPI_Win_flush(rank, m_nodeWin);
            MPI_Win_unlock(rank, m_nodeWin);

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
        constexpr auto nodeSize = sizeof(Node);
        const auto nodeDisplacement = static_cast<MPI_Aint>(nodeAddress.offset * nodeSize);
        const MPI_Aint nodeOffset = MPI_Aint_add(m_pNodeArrAddresses[nodeAddress.rank],
                                                 nodeDisplacement
        );
        int32_t acquiredField{1};
        MPI_Win_lock(MPI_LOCK_SHARED, nodeAddress.rank, 0, m_nodeWin);
        do {
            MPI_Fetch_and_op(&acquiredField,
                             &acquiredField,
                             MPI_UINT32_T,
                             nodeAddress.rank,
                             nodeOffset,
                             MPI_REPLACE,
                             m_nodeWin
            );
            MPI_Win_flush(nodeAddress.rank, m_nodeWin);
        }
        while(acquiredField != 1);
        MPI_Win_unlock(nodeAddress.rank, m_nodeWin);
    }

    void InnerStack::pop(const std::function<void(GlobalAddress)>& getDataCallback)
    {
        {
            MPI_Win_lock(MPI_LOCK_SHARED, m_headRank, 0, m_headWin);
            MPI_Fetch_and_op(&m_pHeadCountedNodePtr,
                             &m_pHeadCountedNodePtr,
                             MPI_UINT64_T,
                             m_headRank,
                             m_headAddress,
                             MPI_NO_OP,
                             m_headWin
            );
            MPI_Win_flush(m_headRank, m_headWin);
            MPI_Win_unlock(m_headRank, m_headWin);
        }

        CountedNodePtr oldHeadCountedNodePtr = *m_pHeadCountedNodePtr;

        for (;;)
        {
            increaseHeadCount(oldHeadCountedNodePtr);
            GlobalAddress nodeAddress = {
                    .offset = static_cast<uint64_t>(oldHeadCountedNodePtr.getOffset()),
                    .rank = oldHeadCountedNodePtr.getRank()
            };
            if (isGlobalAddressDummy(nodeAddress))
                return;

            Node node;
            constexpr auto nodeSize = sizeof(Node);
            const auto nodeDisplacement = static_cast<MPI_Aint>(nodeAddress.offset * nodeSize);
            const MPI_Aint nodeOffset = MPI_Aint_add(m_pNodeArrAddresses[nodeAddress.rank],
                                                     nodeDisplacement
            );

            MPI_Win_lock(MPI_LOCK_SHARED, nodeAddress.rank, 0, m_nodeWin);
            MPI_Get(&node,
                    nodeSize,
                    MPI_UNSIGNED_CHAR,
                    nodeAddress.rank,
                    nodeOffset,
                    nodeSize,
                    MPI_UNSIGNED_CHAR,
                    m_nodeWin
            );
            MPI_Win_flush(nodeAddress.rank, m_nodeWin);
            MPI_Win_unlock(nodeAddress.rank, m_nodeWin);

            auto& newCountedNodePtr = node.getCountedNodePtr();
            CountedNodePtr resCountedNodePtr;

            MPI_Win_lock(MPI_LOCK_SHARED, m_headRank, 0, m_headWin);
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 &oldHeadCountedNodePtr,
                                 &resCountedNodePtr,
                                 MPI_UINT64_T,
                                 m_headRank,
                                 m_headAddress,
                                 m_headWin
            );
            MPI_Win_flush(m_headRank, m_headWin);
            MPI_Win_unlock(m_headRank, m_headWin);

            if (resCountedNodePtr == oldHeadCountedNodePtr)
            {
                getDataCallback(nodeAddress);

                const auto internalCounterRank          = static_cast<int>(nodeAddress.rank);
                const auto internalCounterDisplacement  = static_cast<MPI_Aint>(nodeAddress.offset * sizeof(Node) + sizeof(int32_t));
                const auto internalCounterOffset        = MPI_Aint_add(m_pNodeArrAddresses[nodeAddress.rank],
                                                                   internalCounterDisplacement
                );

                const auto externalCount    = static_cast<int32_t>(oldHeadCountedNodePtr.getExternalCounter());
                const int32_t countIncrease = externalCount - 2;
                int32_t resInternalCount{0};

                MPI_Win_lock(MPI_LOCK_SHARED, internalCounterRank, 0, m_nodeWin);
                MPI_Fetch_and_op(
                        &countIncrease,
                        &resInternalCount,
                        MPI_INT32_T,
                        internalCounterRank,
                        internalCounterOffset,
                        MPI_SUM,
                        m_nodeWin
                );
                MPI_Win_flush(internalCounterRank, m_nodeWin);
                MPI_Win_unlock(internalCounterRank, m_nodeWin);

                if (resInternalCount == -countIncrease)
                    releaseNode(nodeAddress);
            }
            else
            {
                const auto internalCounterRank          = static_cast<int>(nodeAddress.rank);
                const auto internalCounterDisplacement  = static_cast<MPI_Aint>(nodeAddress.offset * sizeof(Node) + sizeof(int32_t));
                const auto internalCounterOffset        = MPI_Aint_add(m_pNodeArrAddresses[nodeAddress.rank],
                                                                       internalCounterDisplacement
                );

                const int64_t countIncrease{-1};
                int64_t resInternalCount{0};
                MPI_Win_lock(MPI_LOCK_SHARED, internalCounterRank, 0, m_nodeWin);
                MPI_Fetch_and_op(
                        &countIncrease,
                        &resInternalCount,
                        MPI_INT32_T,
                        internalCounterRank,
                        internalCounterOffset,
                        MPI_SUM,
                        m_nodeWin
                );
                MPI_Win_flush(internalCounterRank, m_nodeWin);
                MPI_Win_unlock(internalCounterRank, m_nodeWin);

                if (resInternalCount == 1)
                    releaseNode(nodeAddress);
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

        MPI_Win_lock(MPI_LOCK_SHARED, m_headRank, 0, m_headWin);
        do
        {
            newCountedNodePtr = oldCountedNodePtr;
            newCountedNodePtr.incExternalCounter();
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 m_pHeadCountedNodePtr,
                                 &oldCountedNodePtr,
                                 MPI_UINT64_T,
                                 m_headRank,
                                 0,
                                 m_headWin
            );
            MPI_Win_flush(m_headRank, m_headWin);
            SPDLOG_LOGGER_TRACE(m_logger,
                                "rank %d executed CAS in 'increaseHeadCount'",
                                m_rank
            );
        }
        while (oldCountedNodePtr != *m_pHeadCountedNodePtr);
        MPI_Win_unlock(m_headRank, m_headWin);

        oldCountedNodePtr.setExternalCounter(newCountedNodePtr.getExternalCounter());

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'increaseHeadCount'",
                            m_rank
        );
    }

    InnerStack::InnerStack(MPI_Comm comm, MPI_Info info, int headRank, bool t_centralized, size_t t_elemsUpLimit,
                           std::shared_ptr<spdlog::logger> t_logger)
    :
    m_elemsUpLimit(t_elemsUpLimit),
    m_centralized(t_centralized),
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
        allocateProprietaryData(comm, headRank);
        initStackWithDummy(headRank);
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

    void InnerStack::allocateProprietaryData(MPI_Comm comm, int headRank)
    {
        if (m_centralized)
        {
            if (m_rank == headRank)
            {
                constexpr auto nodeSize = static_cast<MPI_Aint>(sizeof(Node));
                const auto nodesSize    = static_cast<MPI_Aint>(nodeSize * m_elemsUpLimit);
                {
                    auto mpiStatus = MPI_Alloc_mem(nodesSize, MPI_INFO_NULL,
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
                    const auto winAttachSize = static_cast<MPI_Aint>(m_elemsUpLimit);
                    auto mpiStatus = MPI_Win_attach(m_nodeWin, m_pNodeArr, winAttachSize);
                    if (mpiStatus != MPI_SUCCESS)
                        throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
                }
                m_pNodeArrAddresses = std::make_unique<MPI_Aint[]>(1);
                MPI_Get_address(m_pNodeArr, m_pNodeArrAddresses.get());
            }

            auto mpiStatus = MPI_Bcast(m_pNodeArrAddresses.get(), 1, MPI_AINT, headRank, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast node array address", __FILE__, __func__ , __LINE__, mpiStatus);
        }
        else
        {
            constexpr auto nodeSize = static_cast<MPI_Aint>(sizeof(Node));
            const auto nodesSize    = static_cast<MPI_Aint>(nodeSize * m_elemsUpLimit);
            {
                auto mpiStatus = MPI_Alloc_mem(nodesSize, MPI_INFO_NULL,
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
                const auto winAttachSize = static_cast<MPI_Aint>(m_elemsUpLimit);
                auto mpiStatus = MPI_Win_attach(m_nodeWin, m_pNodeArr, winAttachSize);
                if (mpiStatus != MPI_SUCCESS)
                    throw custom_mpi::MpiException("failed to attach RMA window", __FILE__, __func__, __LINE__, mpiStatus);
            }
            int procNum{0};
            MPI_Comm_size(comm, &procNum);
            m_pNodeArrAddresses = std::make_unique<MPI_Aint[]>(procNum);
            MPI_Get_address(m_pNodeArr, &m_pNodeArrAddresses[m_rank]);

            for (int i = 0; i < procNum; ++i)
            {
                if (i == m_rank)
                {
                    auto mpiStatus = MPI_Bcast(&m_pNodeArrAddresses[i], 1, MPI_AINT, m_rank, comm);
                    if (mpiStatus != MPI_SUCCESS)
                        throw custom_mpi::MpiException("failed to broadcast node array base address", __FILE__, __func__ , __LINE__, mpiStatus);
                }
            }
        }


        if (m_rank == headRank)
        {
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
            MPI_Get_address(m_pHeadCountedNodePtr, &m_headAddress);
            m_headRank      = 1;
        }

        {
            auto mpiStatus = MPI_Bcast(&m_headAddress, 1, MPI_AINT, headRank, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast head address", __FILE__, __func__ , __LINE__, mpiStatus);
        }
    }

    void InnerStack::initStackWithDummy(int headRank) const
    {
        if (m_rank == headRank)
        {
            MPI_Win_lock(MPI_LOCK_SHARED, m_headRank, 0, m_headWin);
            CountedNodePtr dummyCountedNodePtr;
            MPI_Put(&dummyCountedNodePtr,
                    1,
                    MPI_UINT64_T,
                    m_headRank,
                    0,
                    1,
                    MPI_UINT64_T,
                    m_headWin
            );
            MPI_Win_flush(m_headRank, m_headWin);
            MPI_Win_unlock(m_headRank, m_headWin);
        }
    }

    size_t InnerStack::getElemsUpLimit() const
    {
        return m_elemsUpLimit;
    }
} // ref_counting