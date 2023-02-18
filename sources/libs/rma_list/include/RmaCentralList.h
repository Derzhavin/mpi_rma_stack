//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_RMACENTRALLIST_H
#define SOURCES_RMACENTRALLIST_H

#include <mpi.h>
#include <memory>

#include "IList.h"
#include "CentralNode.h"
#include "rma.h"
#include "MpiWinWrapper.h"

namespace rma_list
{
    namespace custom_mpi = custom_mpi_extensions;

    template<typename T>
    class RmaCentralList: public list_interface::IList<RmaCentralList<T>>
    {
        friend class list_interface::IList_traits<rma_list::RmaCentralList<T>>;
        typedef typename list_interface::IList_traits<RmaCentralList>::ValueType ValueType;
        typedef void (*CentralNodeDeleter_t)(CentralNode<T>*);
        
    public:
        constexpr inline static int CENTRAL_RANK{0};
        constexpr inline static int FREE_NODES_LIMIT{10};

        explicit RmaCentralList(MPI_Comm comm, MPI_Datatype t_mpiDataType);
        RmaCentralList(RmaCentralList&) = delete;
        RmaCentralList(RmaCentralList&&)  noexcept = default;
        RmaCentralList& operator=(RmaCentralList&) = delete;
        RmaCentralList& operator=(RmaCentralList&&)  noexcept = default;
        ~RmaCentralList();

    private:
        void pushBackImpl(T& data);
        void popBackImpl();
        T& tailImpl();
        size_t getSizeImpl();
        bool isEmptyImpl();

        bool isCentralRank();
        bool isShouldFreeMemory();
        bool isFreeNodesLimitAchieved();
        void tryFreeNodes();
        void freeNodes();

    private:
        MPI_Datatype m_mpiDataType;
        int m_rank{-1};
        custom_mpi::MpiWinWrapper m_mpiWinWrapper;
        std::unique_ptr<CentralNode<T>,CentralNodeDeleter_t> m_pTop{nullptr, nullptr};
    };

    template<typename T>
    RmaCentralList<T>::RmaCentralList(MPI_Comm comm, MPI_Datatype t_mpiDataType)
    :
    m_mpiDataType(t_mpiDataType),
    m_mpiWinWrapper(MPI_INFO_NULL, MPI_COMM_WORLD)
    {
        auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__ , __LINE__, mpiStatus);

        if (isCentralRank())
        {
            CentralNode<T> *pTop{nullptr};
            const auto mpiWin = m_mpiWinWrapper.getMpiWin();
            const auto disp = custom_mpi::createObjectInRmaMemory<CentralNode<T>>(mpiWin, pTop, 0);
        }
    }

    template<typename T>
    RmaCentralList<T>::~RmaCentralList()
    {
        freeNodes();
    }

    template<typename T>
    void RmaCentralList<T>::pushBackImpl(T& data)
    {
        tryFreeNodes();
    }

    template<typename T>
    void RmaCentralList<T>::popBackImpl()
    {
        tryFreeNodes();
    }

    template<typename T>
    T& RmaCentralList<T>::tailImpl()
    {
        T t;
        return t;
    }

    template<typename T>
    size_t RmaCentralList<T>::getSizeImpl()
    {
        return 0;
    }

    template<typename T>
    bool RmaCentralList<T>::isEmptyImpl()
    {
        return true;
    }

    template<typename T>
    bool RmaCentralList<T>::isCentralRank()
    {
        return m_rank == CENTRAL_RANK;
    }

    template<typename T>
    bool RmaCentralList<T>::isFreeNodesLimitAchieved()
    {
        return FREE_NODES_LIMIT > getSizeImpl();
    }

    template<typename T>
    bool RmaCentralList<T>::isShouldFreeMemory()
    {
        return isCentralRank() && isFreeNodesLimitAchieved();
    }

    template<typename T>
    void RmaCentralList<T>::tryFreeNodes()
    {
        if(isShouldFreeMemory())
        {
            
        }
    }

    template<typename T>
    void RmaCentralList<T>::freeNodes()
    {
        if (isCentralRank())
        {

        }
    }
} // rma_list

namespace list_interface
{
    template<typename T>
    struct IList_traits<rma_list::RmaCentralList <T>>
    {
        friend class IList<rma_list::RmaCentralList<T>>;
        friend class rma_list::RmaCentralList<T>;
        typedef T ValueType;

    private:
        static void pushBackImpl(rma_list::RmaCentralList<T>& list, T& value)
        {
            list.pushBackImpl(value);
        }
        static void popBackImpl(rma_list::RmaCentralList<T>& list)
        {
            list.popBackImpl();
        }
        static ValueType& tailImpl(rma_list::RmaCentralList<T>& list)
        {
            return list.tailImpl();
        }
        static bool getSizeImpl(rma_list::RmaCentralList<T>& list)
        {
            return list.getSizeImpl();
        }

        static bool isEmptyImpl(rma_list::RmaCentralList<T>& list)
        {
            return list.isEmptyImpl();
        }
    };
}

#endif //SOURCES_RMACENTRALLIST_H
