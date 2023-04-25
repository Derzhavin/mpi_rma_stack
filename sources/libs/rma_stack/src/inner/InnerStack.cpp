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
        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started 'allocatePush'",
                            m_rank
        );

        auto dataAddress = acquireNode(rank);

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d acquired free node in 'allocatePush'",
                            m_rank
        );

        if (isDummyAddress(dataAddress))
            return dataAddress;

        CountedNodePtr newNode;
        newNode.setRank(dataAddress.offset);
        newNode.setOffset(dataAddress.offset);

        newNode.incExternalCounter();

        CountedNodePtr resultNode;

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started new head pushing in 'allocatePush'",
                            m_rank
        );

        MPI_Win_lock_all(0, m_headWin);

        MPI_Fetch_and_op(&resultNode,
                         &resultNode,
                         MPI_UINT64_T,
                         CENTRAL_RANK,
                         0,
                         MPI_NO_OP,
                         m_headWin
        );
        do
        {
            *m_pHeadCountedNodePtr = resultNode;
            MPI_Compare_and_swap(&newNode,
                                 m_pHeadCountedNodePtr,
                                 &resultNode,
                                 MPI_UINT64_T,
                                 CENTRAL_RANK,
                                 0,
                                 m_headWin
            );
            MPI_Win_flush(CENTRAL_RANK, m_headWin);
            SPDLOG_LOGGER_TRACE(m_logger,
                                "rank %d executed CAS in 'allocatePush'",
                                m_rank
            );
        }
        while (resultNode != *m_pHeadCountedNodePtr);

        MPI_Win_unlock_all(m_headWin);

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d finished 'allocatePush'",
                            m_rank
        );
        return dataAddress;
    }

    DataAddress InnerStack::acquireNode(uint64_t rank) const
    {
        DataAddress dataAddress = {
                .rank = CountedNodePtrDummyRank
        };

        CountedNodePtr newNode;
        if (!newNode.setRank(rank))
        {
            return dataAddress;
        }

        Node oldNode;
        Node resNode;
        for (MPI_Aint i = 0; i < m_elemsUpLimit; ++i)
        {
            MPI_Win_lock_all(0, m_nodeWin);
            MPI_Compare_and_swap(&newNode,
                                 &oldNode,
                                 &resNode,
                                 MPI_UINT64_T,
                                 CENTRAL_RANK,
                                 m_nodeArrBaseAddress,
                                 m_nodeWin
            );
            MPI_Win_flush_all(m_nodeWin);
            MPI_Win_unlock_all(m_nodeWin);

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
        MPI_Win_lock_all(0, m_headWin);

        MPI_Fetch_and_op(&m_pHeadCountedNodePtr,
                         &m_pHeadCountedNodePtr,
                         MPI_UINT64_T,
                         CENTRAL_RANK,
                         0,
                         MPI_NO_OP,
                         m_headWin
        );
        MPI_Win_unlock_all(m_headWin);

        CountedNodePtr oldHead = *m_pHeadCountedNodePtr;

        for (;;)
        {
            increaseHeadCount(oldHead);
            DataAddress nodePtr = {
                    .offset = static_cast<uint64_t>(oldHead.getOffset()),
                    .rank = oldHead.getRank()
            };
            if (isDummyAddress(nodePtr))
                return nodePtr;

            Node node;
            MPI_Win_lock_all(0, m_nodeWin);
            MPI_Get(&node,
                    2,
                    MPI_UINT64_T,
                    nodePtr.rank,
                    nodePtr.offset,
                    2,
                    MPI_UINT64_T,
                    m_nodeWin
            );
            MPI_Win_unlock_all(m_nodeWin);

            CountedNodePtr newNode, resNode;
            newNode.setRank(node.getRank());
            newNode.setOffset(node.getOffset());

            MPI_Win_lock_all(0, m_headWin);
            MPI_Compare_and_swap(&newNode,
                                 &oldHead,
                                 &resNode,
                                 MPI_UINT64_T,
                                 m_headAddress.rank,
                                 m_headAddress.offset,
                                 m_headWin
            );
            MPI_Win_flush_all(m_headWin);
            MPI_Win_unlock_all(m_headWin);

            if (resNode == oldHead)
            {
                DataAddress nodeAddress = {
                        .offset = static_cast<uint64_t>(newNode.getOffset()),
                        .rank = newNode.getRank()
                };

                DataAddress internalCounterAddress = {
                        .offset = nodeAddress.offset + 1,
                        .rank = nodeAddress.rank
                };
                int64_t const countIncrease = oldHead.getExternalCounter() - 2;
                int64_t resultInternalCount{0};
                MPI_Fetch_and_op(
                        &countIncrease,
                        &resultInternalCount,
                        MPI_INT64_T,
                        internalCounterAddress.rank,
                        internalCounterAddress.offset,
                        MPI_SUM,
                        m_nodeWin
                );

                if (resultInternalCount == -countIncrease)
                {
                    releaseNode(nodeAddress);
                }
            }
            DataAddress nodeAddress = {
                    .offset = static_cast<uint64_t>(oldHead.getOffset()),
                    .rank = oldHead.getRank()
            };

            DataAddress internalCounterAddress = {
                    .offset = nodeAddress.offset + 1,
                    .rank = nodeAddress.rank
            };
            int64_t const countIncrease{-1};
            int64_t resultInternalCount{0};
            MPI_Fetch_and_op(
                    &countIncrease,
                    &resultInternalCount,
                    MPI_INT64_T,
                    internalCounterAddress.rank,
                    internalCounterAddress.offset,
                    MPI_SUM,
                    m_nodeWin
            );

            if (resultInternalCount == 1)
            {
                releaseNode(nodeAddress);
            }
        }
    }

    void InnerStack::increaseHeadCount(CountedNodePtr &oldCounter)
    {
        CountedNodePtr newCounter;

        SPDLOG_LOGGER_TRACE(m_logger,
                            "rank %d started 'increaseHeadCount'",
                            m_rank
        );

        MPI_Win_lock_all(0, m_headWin);

        do
        {
            newCounter = oldCounter;
            newCounter.incExternalCounter();
            MPI_Compare_and_swap(&newCounter,
                                 m_pHeadCountedNodePtr,
                                 &oldCounter,
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
        while (oldCounter != *m_pHeadCountedNodePtr);
        MPI_Win_unlock_all(m_headWin);

        oldCounter.setExternalCounter(newCounter.getExternalCounter());

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
            MPI_Get_address(m_pNodeArr, &m_nodeArrBaseAddress);

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
            m_headAddress.offset    = address;
            m_headAddress.rank      = 1;
        }

        {
            auto mpiStatus = MPI_Bcast(&m_nodeArrBaseAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
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
            CountedNodePtr dummy;
            MPI_Put(&dummy, 1, MPI_UINT64_T, CENTRAL_RANK, 0, 1, MPI_UINT64_T, m_headWin);
            MPI_Win_unlock_all(m_headWin);
        }
    }

    void InnerStack::releaseNode(DataAddress nodeAddress) const
    {
        DataAddress address = {
                .offset = 0,
                .rank = NodeDummyRank
        };

        MPI_Win_lock_all(0, m_nodeWin);
        MPI_Put(&address,
                1,
                MPI_UINT64_T,
                nodeAddress.rank,
                nodeAddress.offset,
                1,
                MPI_UINT64_T,
                m_nodeWin
        );
        MPI_Win_unlock_all(m_nodeWin);
    }

    size_t InnerStack::getElemsUpLimit() const
    {
        return m_elemsUpLimit;
    }
} // ref_counting