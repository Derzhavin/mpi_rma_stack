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
        m_logger->trace("started 'push'");

        auto nodeAddress = acquireNode(m_centralized ? HEAD_RANK : m_rank);
        if (isGlobalAddressDummy(nodeAddress))
        {
            m_logger->trace("failed to find free node in 'tryPush'");
            return false;
        }
        {
            const auto r = nodeAddress.rank;
            const auto o = nodeAddress.offset;
            m_logger->trace("acquired free node (rank - {}, offset - {}) in 'push'", r, o);
        }

        putDataCallback(nodeAddress);
        m_logger->trace("put data in 'push'");

        CountedNodePtr newCountedNodePtr;
        newCountedNodePtr.setRank(nodeAddress.rank);
        newCountedNodePtr.setOffset(nodeAddress.offset);
        newCountedNodePtr.incExternalCounter();

        CountedNodePtr resCountedNodePtr;

        m_logger->trace("started new head pushing in 'push'");

        MPI_Win_lock(MPI_LOCK_SHARED, HEAD_RANK, 0, m_headWin);
        MPI_Fetch_and_op(&resCountedNodePtr,
                         &resCountedNodePtr,
                         MPI_UINT64_T,
                         HEAD_RANK,
                         m_headAddress,
                         MPI_NO_OP,
                         m_headWin
        );
        MPI_Win_flush(HEAD_RANK, m_headWin);
        m_logger->trace("fetched head (rank - {}, offset - {})", resCountedNodePtr.getRank(), resCountedNodePtr.getOffset());

        CountedNodePtr oldCountedNodePtr;
        do
        {
            oldCountedNodePtr = resCountedNodePtr;
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 &oldCountedNodePtr,
                                 &resCountedNodePtr,
                                 MPI_UINT64_T,
                                 HEAD_RANK,
                                 m_headAddress,
                                 m_headWin
            );
            MPI_Win_flush(HEAD_RANK, m_headWin);
            m_logger->trace("executed CAS in 'push'");
        }
        while (resCountedNodePtr != oldCountedNodePtr);
        MPI_Win_unlock(HEAD_RANK, m_headWin);

        m_logger->trace("finished 'push'");
        return true;
    }

    GlobalAddress InnerStack::acquireNode(int rank) const
    {
        GlobalAddress nodeGlobalAddress = {0, DummyRank, 0};

        if (!isValidRank(rank))
            return nodeGlobalAddress;

        uint32_t newAcquiredField{1};
        uint32_t oldAcquiredField{0};
        uint32_t resAcquiredField{0};

        for (MPI_Aint i = 0; i < static_cast<MPI_Aint>(m_elemsUpLimit); ++i)
        {
            constexpr auto nodeSize = static_cast<MPI_Aint>(sizeof(Node));
            assert(nodeSize == 16);
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

            m_logger->trace("resAcquiredField = {} of (rank - {}, offset - {})) in 'acquireNode'",
                            resAcquiredField,
                            rank,
                            i
            );
            assert(resAcquiredField == 0 || resAcquiredField == 1);
            if (!resAcquiredField)
            {
                nodeGlobalAddress.rank = rank;
                nodeGlobalAddress.offset = i;
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
        {
            const auto r = nodeAddress.rank;
            const auto o = nodeAddress.offset;
            m_logger->trace("started to release node (rank - {}, offset - {}", r, o);
        }
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
        {
            const auto r = nodeAddress.rank;
            const auto o = nodeAddress.offset;
            m_logger->trace("released node (rank - {}, offset - {}", r, o);
        }
    }

    void InnerStack::pop(const std::function<void(GlobalAddress)>& getDataCallback)
    {
        CountedNodePtr oldHeadCountedNodePtr;
        
        MPI_Win_lock(MPI_LOCK_SHARED, HEAD_RANK, 0, m_headWin);
        MPI_Fetch_and_op(&oldHeadCountedNodePtr,
                         &oldHeadCountedNodePtr,
                         MPI_UINT64_T,
                         HEAD_RANK,
                         m_headAddress,
                         MPI_NO_OP,
                         m_headWin
        );
        MPI_Win_flush(HEAD_RANK, m_headWin);
        MPI_Win_unlock(HEAD_RANK, m_headWin);

        for (;;)
        {
            increaseHeadCount(oldHeadCountedNodePtr);
            GlobalAddress nodeAddress = {
                    oldHeadCountedNodePtr.getOffset(),
                    oldHeadCountedNodePtr.getRank(),
                    0
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

            MPI_Win_lock(MPI_LOCK_SHARED, HEAD_RANK, 0, m_headWin);
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 &oldHeadCountedNodePtr,
                                 &resCountedNodePtr,
                                 MPI_UINT64_T,
                                 HEAD_RANK,
                                 m_headAddress,
                                 m_headWin
            );
            MPI_Win_flush(HEAD_RANK, m_headWin);
            MPI_Win_unlock(HEAD_RANK, m_headWin);

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
        CountedNodePtr resCountedNodePtr;
        m_logger->trace("started 'increaseHeadCount'");

        MPI_Win_lock(MPI_LOCK_SHARED, HEAD_RANK, 0, m_headWin);
        do
        {
            newCountedNodePtr = oldCountedNodePtr;
            newCountedNodePtr.incExternalCounter();
            MPI_Compare_and_swap(&newCountedNodePtr,
                                 &oldCountedNodePtr,
                                 &resCountedNodePtr,
                                 MPI_UINT64_T,
                                 HEAD_RANK,
                                 m_headAddress,
                                 m_headWin
            );
            MPI_Win_flush(HEAD_RANK, m_headWin);
            m_logger->trace("executed CAS in 'increaseHeadCount'");
        }
        while (oldCountedNodePtr != resCountedNodePtr);
        MPI_Win_unlock(HEAD_RANK, m_headWin);

        oldCountedNodePtr.setExternalCounter(newCountedNodePtr.getExternalCounter());

        m_logger->trace("finished 'increaseHeadCount'");
    }

    InnerStack::InnerStack(MPI_Comm comm, MPI_Info info, bool t_centralized, size_t t_elemsUpLimit,
                           std::shared_ptr<spdlog::logger> t_logger)
    :
    m_elemsUpLimit(t_elemsUpLimit),
    m_centralized(t_centralized),
    m_logger(std::move(t_logger))
    {
        m_logger->trace("getting rank");
        {
            auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to get rank", __FILE__, __func__, __LINE__, mpiStatus);
        }
        m_logger->trace("got rank {}", m_rank);
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
        MPI_Barrier(comm);
        m_logger->trace("finished InnerStack construction");
    }

    void InnerStack::release()
    {
        MPI_Free_mem(m_pNodeArr);
        m_pNodeArr = nullptr;
        m_logger->trace("freed up node arr RMA memory");

        MPI_Win_free(&m_nodeWin);
        m_logger->trace("freed up node win RMA memory");

        MPI_Free_mem(m_pHeadCountedNodePtr);
        m_pHeadCountedNodePtr = nullptr;
        m_logger->trace("freed up head pointer RMA memory");

        MPI_Win_free(&m_headWin);
        m_logger->trace("freed up head win RMA memory");
    }

    void InnerStack::allocateProprietaryData(MPI_Comm comm)
    {
        if (m_centralized)
        {
            m_logger->trace("started to allocate node proprietary data");

            m_pNodeArrAddresses = std::make_unique<MPI_Aint[]>(1);

            if (m_rank == HEAD_RANK)
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
                MPI_Get_address(m_pNodeArr, m_pNodeArrAddresses.get());
            }
            m_logger->trace("allocated node proprietary data");

            m_logger->trace("started to broadcast node arr addresses");
            auto mpiStatus = MPI_Bcast(m_pNodeArrAddresses.get(), 1, MPI_AINT, HEAD_RANK, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast node array address", __FILE__, __func__ , __LINE__, mpiStatus);
            m_logger->trace("finished broadcasting node arr addresses");
        }
        else
        {
            m_logger->trace("started to allocate node proprietary data");

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

            m_logger->trace("allocated node proprietary data");

            m_logger->trace("started to broadcast node arr addresses");

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
            m_logger->trace("finished broadcasting node arr addresses");
        }

        if (m_rank == HEAD_RANK)
        {
            m_logger->trace("started to allocate head data");

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
            m_logger->trace("finished allocating head data");
        }

        {
            m_logger->trace("started to broadcast head address");
            auto mpiStatus = MPI_Bcast(&m_headAddress, 1, MPI_AINT, HEAD_RANK, comm);
            if (mpiStatus != MPI_SUCCESS)
                throw custom_mpi::MpiException("failed to broadcast head address", __FILE__, __func__ , __LINE__, mpiStatus);
            m_logger->trace("finished broadcasting head address");
        }
    }

    void InnerStack::initStackWithDummy() const
    {
        if (m_rank == HEAD_RANK)
        {
            m_logger->trace("started to initialize head with dummy");
            MPI_Win_lock(MPI_LOCK_SHARED, HEAD_RANK, 0, m_headWin);
            CountedNodePtr dummyCountedNodePtr;
            MPI_Put(&dummyCountedNodePtr,
                    1,
                    MPI_UINT64_T,
                    HEAD_RANK,
                    m_headAddress,
                    1,
                    MPI_UINT64_T,
                    m_headWin
            );
            MPI_Win_flush(HEAD_RANK, m_headWin);
            MPI_Win_unlock(HEAD_RANK, m_headWin);
            m_logger->trace("initialized head with dummy");
        }
    }

    size_t InnerStack::getElemsUpLimit() const
    {
        return m_elemsUpLimit;
    }
} // ref_counting