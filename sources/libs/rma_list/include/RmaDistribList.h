//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_RMACENTRALLIST_H
#define SOURCES_RMACENTRALLIST_H

#include <mpi.h>
#include <memory>

#include "IList.h"
#include "DistribNode.h"
#include "rma.h"
#include "MpiWinWrapper.h"

namespace rma_list
{
    namespace custom_mpi = custom_mpi_extensions;

    template<typename T>
    class RmaDistribList: public list_interface::IList<RmaDistribList<T>>
    {
        friend class list_interface::IList_traits<rma_list::RmaDistribList<T>>;
        typedef typename list_interface::IList_traits<RmaDistribList>::ValueType ValueType;
        typedef void (*DistribNodeDeleter_t)(DistribNode<T>*);

    public:
        explicit RmaDistribList(MPI_Comm comm, MPI_Datatype t_mpiDataType);
        RmaDistribList(RmaDistribList&) = delete;
        RmaDistribList(RmaDistribList&&)  noexcept = default;
        RmaDistribList& operator=(RmaDistribList&) = delete;
        RmaDistribList& operator=(RmaDistribList&&)  noexcept = default;
        ~RmaDistribList() = default;

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
        std::unique_ptr<DistribNode<T>,DistribNodeDeleter_t> m_pTop{nullptr, nullptr};
    };

    template<typename T>
    RmaDistribList<T>::RmaDistribList(MPI_Comm comm, MPI_Datatype t_mpiDataType)
    :
    m_mpiWinWrapper(MPI_INFO_NULL, MPI_COMM_WORLD)
    {
        auto mpiStatus = MPI_Comm_rank(comm, &m_rank);
        if (mpiStatus != MPI_SUCCESS)
            throw custom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__ , __LINE__, mpiStatus);

        DistribNode<T> *pTop{nullptr};
        const auto mpiWin = m_mpiWinWrapper.getMpiWin();
        const auto disp = custom_mpi::createObjectInRmaMemory<DistribNode<T>>(mpiWin, pTop, 0);
        m_pTop = std::unique_ptr<DistribNode<T>,DistribNodeDeleter_t>(pTop, &custom_mpi::freeRmaMemory<DistribNode<T>>);
    }
} // rma_list

namespace list_interface
{
    template<typename T>
    struct IList_traits<rma_list::RmaDistribList <T>>
    {
        friend class IList<rma_list::RmaDistribList<T>>;
        friend class rma_list::RmaDistribList<T>;
        typedef T ValueType;

    private:
        static void pushBackImpl(rma_list::RmaDistribList<T>& list, T& value)
        {
            list.pushBackImpl(value);
        }
        static void popBackImpl(rma_list::RmaDistribList<T>& list)
        {
            list.popBackImpl();
        }
        static ValueType& tailImpl(rma_list::RmaDistribList<T>& list)
        {
            return list.tailImpl();
        }
        static bool isEmptyImpl(rma_list::RmaDistribList<T>& list)
        {
            return list.isEmptyImpl();
        }
    };
}

#endif //SOURCES_RMACENTRALLIST_H
