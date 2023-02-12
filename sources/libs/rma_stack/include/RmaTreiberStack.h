//
// Created by denis on 22.01.23.
//

#ifndef SOURCES_RMATREIBERSTACK_H
#define SOURCES_RMATREIBERSTACK_H

#include <mpi.h>

#include "IStack.h"
#include "MpiException.h"

namespace rma_stack
{
    template<typename T>
    class RmaTreiberStack: public stack_interface::IStack<RmaTreiberStack<T>>
    {
        friend class stack_interface::IStack_traits<rma_stack::RmaTreiberStack<T>>;
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
            throw custom_mpi_extensions::MpiException("failed to get rank", __FILE__, __func__ , __LINE__, mpiStatus);
    }
} // rma_stack


namespace stack_interface
{
    template<typename T>
    struct IStack_traits<rma_stack::RmaTreiberStack <T>>
    {
        friend class IStack<rma_stack::RmaTreiberStack<T>>;
        friend class rma_stack::RmaTreiberStack<T>;
        typedef T ValueType;

    private:
        static void pushImpl(rma_stack::RmaTreiberStack<T>& stack, T& value)
        {
            stack.pushImpl(value);
        }
        static void popImpl(rma_stack::RmaTreiberStack<T>& stack)
        {
            stack.popImpl();
        }
        static ValueType& topImpl(rma_stack::RmaTreiberStack<T>& stack)
        {
            return stack.topImpl();
        }
        static bool isEmptyImpl(rma_stack::RmaTreiberStack<T>& stack)
        {
            return stack.isEmptyImpl();
        }
    };
}
#endif //SOURCES_RMATREIBERSTACK_H
