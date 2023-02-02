//
// Created by denis on 22.01.23.
//

#ifndef SOURCES_RMATREIBERSTACK_HPP
#define SOURCES_RMATREIBERSTACK_HPP

#include <mpi.h>

#include "IStack.hpp"
#include "MpiException.hpp"

namespace rma_treiber_stack
{
    template<typename T>
    class RmaTreiberStack: public stack_interface::IStack<RmaTreiberStack<T>>
    {
        friend class stack_interface::IStack_traits<rma_treiber_stack::RmaTreiberStack<T>>;
        typedef typename stack_interface::IStack_traits<RmaTreiberStack>::ValueType ValueType;

    public:
        explicit RmaTreiberStack(const MPI_Comm& comm);

    private:
        void pushImpl(T& data)
        {

        }
        void popImpl()
        {

        }
        T& topImpl()
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
    };

    template<typename T>
    RmaTreiberStack<T>::RmaTreiberStack(const MPI_Comm& comm)
    {
        auto mpiStatus = MPI_Comm_rank(comm, &m_rank);

        if (mpiStatus != MPI_SUCCESS)
            throw cutom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__ , __LINE__, mpiStatus);
    }
} // rma_treiber_stack


namespace stack_interface
{
    template<typename T>
    class IStack_traits<rma_treiber_stack::RmaTreiberStack < T>>
    {
        friend class IStack<rma_treiber_stack::RmaTreiberStack<T>>;
        friend class rma_treiber_stack::RmaTreiberStack<T>;
        typedef T ValueType;

    private:
        static void pushImpl(rma_treiber_stack::RmaTreiberStack<T>& stack, T& value)
        {
            stack.pushImpl(value);
        }
        static void popImpl(rma_treiber_stack::RmaTreiberStack<T>& stack)
        {
            stack.popImpl();
        }
        static ValueType& topImpl(rma_treiber_stack::RmaTreiberStack<T>& stack)
        {
            return stack.topImpl();
        }
        static bool isEmptyImpl(rma_treiber_stack::RmaTreiberStack<T>& stack)
        {
            return stack.isEmptyImpl();
        }
    };
}
#endif //SOURCES_RMATREIBERSTACK_HPP
