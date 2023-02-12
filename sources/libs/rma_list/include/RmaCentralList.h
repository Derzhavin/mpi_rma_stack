//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_RMACENTRALLIST_H
#define SOURCES_RMACENTRALLIST_H

#include <mpi.h>
#include <memory>

#include "IList.h"
#include "Node.h"
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
        typedef void (*NodeDeleter)(Node<T>*);

    public:
        explicit RmaCentralList(const MPI_Comm& comm);
        RmaCentralList(RmaCentralList&) = delete;
        RmaCentralList(RmaCentralList&&)  noexcept = default;
        RmaCentralList& operator=(RmaCentralList&) = delete;
        RmaCentralList& operator=(RmaCentralList&&)  noexcept = default;
        ~RmaCentralList() = default;

    private:
        void pushBackImpl(T& data)
        {
        }
        void popBackImpl()
        {
        }
        T& tailImpl()
        {
            T t;
            return t;
        }
        bool isEmptyImpl()
        {
            return true;
        }
        
    private:
        int m_rank{-1};
        custom_mpi::MpiWinWrapper m_mpiWinWrapper;
        std::unique_ptr<Node<T>,NodeDeleter> m_pTop{nullptr, nullptr};
    };

    template<typename T>
    RmaCentralList<T>::RmaCentralList(const MPI_Comm& comm)
    :
    m_mpiWinWrapper(MPI_INFO_NULL, MPI_COMM_WORLD)
    {
        CHECK_MPI_STATUS(MPI_Comm_rank(comm, &m_rank), "failed to get rank");

        Node<T> *pTop{nullptr};
        const auto mpiWin = m_mpiWinWrapper.getMpiWin();
        const auto disp = custom_mpi::createObjectInRmaMemory<Node<T>>(mpiWin, pTop, 0);
        m_pTop = std::unique_ptr<Node<T>,NodeDeleter>(pTop, &custom_mpi::freeRmaMemory<Node<T>>);
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
        static bool isEmptyImpl(rma_list::RmaCentralList<T>& list)
        {
            return list.isEmptyImpl();
        }
    };
}

#endif //SOURCES_RMACENTRALLIST_H
