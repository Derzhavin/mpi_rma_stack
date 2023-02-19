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

namespace rma_stack
{
    namespace custom_mpi = custom_mpi_extensions;

    template<typename T>
    class RmaTreiberCentralStack: public stack_interface::IStack<RmaTreiberCentralStack<T>>
    {
        friend class stack_interface::IStack_traits<rma_stack::RmaTreiberCentralStack<T>>;
        typedef typename stack_interface::IStack_traits<RmaTreiberCentralStack>::ValueType ValueType;

        typedef void (*CentralNodeDeleter_t)(CentralNode<T>*);

    public:
        explicit RmaTreiberCentralStack(MPI_Comm comm, MPI_Datatype t_mpiDataType,
                                        const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                        const std::chrono::nanoseconds &t_rBackoffMaxDelay, int t_freeNodesLimit);
        RmaTreiberCentralStack(RmaTreiberCentralStack&) = delete;
        RmaTreiberCentralStack(RmaTreiberCentralStack&&)  noexcept = default;
        RmaTreiberCentralStack& operator=(RmaTreiberCentralStack&) = delete;
        RmaTreiberCentralStack& operator=(RmaTreiberCentralStack&&)  noexcept = default;
        ~RmaTreiberCentralStack();

    private:
        // public interface begin
        void pushImpl(const T &value);
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
        bool tryPush(const T &value);
        std::optional<CentralNode<T>> tryPop();

    public:
        constexpr inline static int CENTRAL_RANK{0};

        const int freeNodesLimit;
        const std::chrono::nanoseconds backoffMinDelay;
        const std::chrono::nanoseconds backoffMaxDelay;

    private:
        MPI_Datatype m_mpiDataType;
        int m_rank{-1};
        custom_mpi::MpiWinWrapper m_mpiWinWrapper;
    };

    template<typename T>
    RmaTreiberCentralStack<T>::RmaTreiberCentralStack(MPI_Comm comm, MPI_Datatype t_mpiDataType,
                                                      const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                      const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                      int t_freeNodesLimit)
            :
            m_mpiDataType(t_mpiDataType),
            m_mpiWinWrapper(MPI_INFO_NULL, MPI_COMM_WORLD),
            backoffMinDelay(t_rBackoffMinDelay),
            backoffMaxDelay(t_rBackoffMaxDelay),
            freeNodesLimit(t_freeNodesLimit)
    {
        auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__ , __LINE__, mpiStatus);

        if (isCentralRank())
        {
            CentralNode<T> *pTop{nullptr};
            const auto mpiWin = m_mpiWinWrapper.getMpiWin();
        }
    }

    template<typename T>
    RmaTreiberCentralStack<T>::~RmaTreiberCentralStack()
    {
        freeNodes();
    }

    template<typename T>
    void RmaTreiberCentralStack<T>::pushImpl(const T &value)
    {
        Backoff backoff(backoffMinDelay, backoffMaxDelay);
        while(!isStopRequested())
        {
            if (tryPush(value))
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
        return rma_stack::CentralNode<T>(T(), 0);
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
    bool RmaTreiberCentralStack<T>::tryPush(const T &value)
    {
        CentralNode<T> oldTop = getTopNode();
        const auto oldTopDisp = (MPI_Aint)MPI_BOTTOM;

        CentralNode<T> newTop(value, oldTopDisp);

        const auto win = m_mpiWinWrapper.getMpiWin();
        void* resulAddr{nullptr};
        auto mpiStatus = MPI_Compare_and_swap(&newTop, &oldTop, resulAddr, m_mpiDataType, CENTRAL_RANK, oldTopDisp, win);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi_extensions::MpiException("failed to execute MPI CAS", __FILE__, __func__ , __LINE__, mpiStatus);

        return &oldTop == resulAddr;
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
