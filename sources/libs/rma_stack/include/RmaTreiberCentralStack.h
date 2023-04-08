//
// Created by denis on 22.01.23.
//

#ifndef SOURCES_RMATREIBERCENTRALSTACK_H
#define SOURCES_RMATREIBERCENTRALSTACK_H

#include <mpi.h>
#include <memory>
#include <optional>

#include "IStack.h"
#include "rma.h"
#include "MpiWinWrapper.h"
#include "CentralNode.h"
#include "Backoff.h"
#include "CentralizedMemoryPool.h"

namespace rma_stack
{
    namespace custom_mpi = custom_mpi_extensions;

    template<typename T>
    class RmaTreiberCentralStack: public stack_interface::IStack<RmaTreiberCentralStack<T>>
    {
        friend class stack_interface::IStack_traits<rma_stack::RmaTreiberCentralStack<T>>;
        typedef typename stack_interface::IStack_traits<RmaTreiberCentralStack>::ValueType ValueType;

        typedef void (*CountedNodeDeleter_t)(CountedNode*);

    public:
        explicit RmaTreiberCentralStack(MPI_Comm comm,
                                        const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                        const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                        int t_freeNodesLimit,
                                        CentralizedMemoryPool<T> &&t_elemsCentralizedMemoryPool);
        RmaTreiberCentralStack(RmaTreiberCentralStack&) = delete;
        RmaTreiberCentralStack(RmaTreiberCentralStack&&)  noexcept = default;
        RmaTreiberCentralStack& operator=(RmaTreiberCentralStack&) = delete;
        RmaTreiberCentralStack& operator=(RmaTreiberCentralStack&&)  noexcept = default;
        ~RmaTreiberCentralStack();

    private:
        // public interface begin
        void pushImpl(const T &rValue);
        T popImpl();
        T& topImpl();
        size_t sizeImpl();
        bool isEmptyImpl();
        // public interface end

        bool isCentralRank();
        bool isShouldFreeMemory();
        bool isFreeNodesLimitAchieved();
        bool isStopRequested();
        void tryFreeNodes();
        void freeNodes();
        CentralNode<T> getTopNode();
        bool tryPush(const T &rValue);
        std::optional<CentralNode<T>> tryPop();

    public:
        constexpr inline static int CENTRAL_RANK{0};

    private:
        int freeNodesLimit;
        std::chrono::nanoseconds backoffMinDelay;
        std::chrono::nanoseconds backoffMaxDelay;

        int m_rank{-1};
        MPI_Aint m_headCountedNodeAddress{(MPI_Aint)MPI_BOTTOM};
        CentralizedMemoryPool<T> m_centralNodesMemoryPool;
        CentralizedMemoryPool<T> m_countedNodesMemoryPool;
    };

    template<typename T>
    RmaTreiberCentralStack<T>::RmaTreiberCentralStack(MPI_Comm comm,
                                                      const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                      const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                      int t_freeNodesLimit,
                                                      CentralizedMemoryPool<T> &&t_elemsCentralizedMemoryPool)
    :
    backoffMinDelay(t_rBackoffMinDelay),
    backoffMaxDelay(t_rBackoffMaxDelay),
    freeNodesLimit(t_freeNodesLimit),
    m_centralNodesMemoryPool(t_elemsCentralizedMemoryPool)
    {
        auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__ , __LINE__, mpiStatus);

        if (isCentralRank())
        {
            m_headCountedNodeAddress = m_countedNodesMemoryPool.allocate();
            auto win = m_centralNodesMemoryPool.getElemsWinWrapper().getMpiWin();
            MPI_Win_lock_all(0, win);
            CountedNode head;
            MPI_Put(&head, 1, );
            MPI_Win_unlock_all(win);
        }
        MPI_Bcast(&m_headCountedNodeAddress, 1, MPI_AINT, CENTRAL_RANK, comm);
    }

    template<typename T>
    RmaTreiberCentralStack<T>::~RmaTreiberCentralStack()
    {
        freeNodes();
    }

    template<typename T>
    void RmaTreiberCentralStack<T>::pushImpl(const T &rValue)
    {
        Backoff backoff(backoffMinDelay, backoffMaxDelay);
        while(!isStopRequested())
        {
            if (tryPush(rValue))
            {
                return;
            }
            else
            {
                backoff.backoff();
            }
        }
    }

    template<typename T>
    T RmaTreiberCentralStack<T>::popImpl()
    {
        Backoff backoff(backoffMinDelay, backoffMaxDelay);
        while (!isStopRequested())
        {
            auto optNode = tryPop();

            if (optNode)
            {
                auto node = optNode.value();
                auto value = std::move(node.getValue());
                return value;
            }
            else
            {
                backoff.backoff();
            }
        }
    }

    template<typename T>
    T &rma_stack::RmaTreiberCentralStack<T>::topImpl() {
        T v{};
        return v;
    }

    template<typename T>
    size_t RmaTreiberCentralStack<T>::sizeImpl()
    {
        return 0;
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::isEmptyImpl()
    {
        return true;
    }

    template<typename T>
    CentralNode<T> rma_stack::RmaTreiberCentralStack<T>::getTopNode()
    {
        return rma_stack::CentralNode<T>(T());
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::isCentralRank()
    {
        return m_rank == CENTRAL_RANK;
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::isFreeNodesLimitAchieved()
    {
        return freeNodesLimit > sizeImpl();
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::isShouldFreeMemory()
    {
        return isCentralRank() && isFreeNodesLimitAchieved();
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::isStopRequested()
    {
        return false;
    }

    template<typename T>
    void RmaTreiberCentralStack<T>::tryFreeNodes()
    {
        if(isShouldFreeMemory())
        {

        }
    }

    template<typename T>
    void RmaTreiberCentralStack<T>::freeNodes()
    {
        if (isCentralRank())
        {

        }
    }

    template<typename T>
    bool RmaTreiberCentralStack<T>::tryPush(const T &rValue)
    {
        const auto newNodeAddress = m_centralNodesMemoryPool.allocate();
        CountedNode newNode(newNodeAddress);
        newNode.incExternalCounter();

        auto win = m_centralNodesMemoryPool.getElemsWinWrapper().getMpiWin();
        MPI_Win_lock_all(0, win);

        CentralNode<T> resulAddr;
        do
        {

        } while(1);

//        auto headAddress = m_pHeadCountedNode->getAddress();
//        auto mpiStatus = MPI_Compare_and_swap(&newTop, &oldTop, &resulAddr, m_mpiDataType, CENTRAL_RANK, oldTopCountedNode.m_address, win);
//        if (mpiStatus != MPI_SUCCESS)
//            throw custom_mpi_extensions::MpiException("failed to execute MPI CAS", __FILE__, __func__ , __LINE__, mpiStatus);
        MPI_Win_unlock_all(win);
        return true;
    }

    template<typename T>
    std::optional<CentralNode<T>> RmaTreiberCentralStack<T>::tryPop()
    {
        std::optional<CentralNode<T>> node;
        return node;
    }
} // rma_stack


namespace stack_interface
{
    template<typename T>
    struct IStack_traits<rma_stack::RmaTreiberCentralStack < T>>
    {
        friend class IStack<rma_stack::RmaTreiberCentralStack < T>>;
        friend class rma_stack::RmaTreiberCentralStack<T>;
        typedef T ValueType;

    private:
        static void pushImpl(rma_stack::RmaTreiberCentralStack<T>& stack, const T &value)
        {
            stack.pushImpl(value);
        }
        static ValueType popImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.popImpl();
        }
        static ValueType& topImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.topImpl();
        }
        static size_t sizeImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.sizeImpl();
        }
        static bool isEmptyImpl(rma_stack::RmaTreiberCentralStack<T>& stack)
        {
            return stack.isEmptyImpl();
        }
    };
}
#endif //SOURCES_RMATREIBERCENTRALSTACK_H
