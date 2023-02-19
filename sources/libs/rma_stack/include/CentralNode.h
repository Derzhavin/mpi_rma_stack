//
// Created by denis on 18.02.23.
//

#ifndef SOURCES_CENTRALNODE_H
#define SOURCES_CENTRALNODE_H

#include <mpi.h>

namespace rma_stack
{
    template<typename T>
    class CentralNode
    {
    public:
        explicit CentralNode(T t_value, MPI_Aint t_nextDisp);
        explicit CentralNode(T t_value);

        T const& getValue() const;
        void setValue(const T& t_rValue);
        [[nodiscard]] MPI_Aint getNext() const;
        void setNext(MPI_Aint t_next);

    private:
        MPI_Aint m_nextDisp;
        T m_value{};
    };

    template<typename T>
    CentralNode<T>::CentralNode(T t_value, MPI_Aint t_nextDisp): m_value(std::move(t_value)), m_nextDisp(t_nextDisp) {}

    template<typename T>
    CentralNode<T>::CentralNode(T t_value): CentralNode(std::move(t_value), (MPI_Aint) MPI_BOTTOM) {}

    template<typename T>
    T const &CentralNode<T>::getValue() const
    {
        return m_value;
    }

    template<typename T>
    void CentralNode<T>::setValue(const T &t_rValue)
    {
        m_value = t_rValue;
    }

    template<typename T>
    MPI_Aint CentralNode<T>::getNext() const
    {
        return m_nextDisp;
    }

    template<typename T>
    void CentralNode<T>::setNext(MPI_Aint t_nextDisp)
    {
        m_nextDisp = t_nextDisp;
    }
} // rma_stack

#endif //SOURCES_CENTRALNODE_H
