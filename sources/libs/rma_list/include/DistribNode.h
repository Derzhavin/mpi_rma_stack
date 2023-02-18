//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_DISTRIBNODE_H
#define SOURCES_DISTRIBNODE_H

#include <mpi.h>

namespace rma_list
{
    struct DistribNodePtr
    {
        int rank;
        MPI_Aint disp;

        void reset();
    };

    void DistribNodePtr::reset()
    {
        disp = (MPI_Aint)MPI_BOTTOM;
        rank = -1;
    }

    template<typename T>
    class DistribNode
    {
    public:
        explicit DistribNode(T t_value, const DistribNodePtr &t_next);
        explicit DistribNode(T t_value);

        T const& getValue() const;
        void setValue(const T& t_value);
        [[nodiscard]] const DistribNodePtr &getNext() const;

        void setNext(const DistribNodePtr &t_next);

    private:
        T m_value{};
        DistribNodePtr m_next{};
    };

    template<typename T>
    DistribNode<T>::DistribNode(T t_value, const DistribNodePtr &t_next):m_value(t_value), m_next(t_next) {}

    template<typename T>
    DistribNode<T>::DistribNode(T t_value): DistribNode(t_value, {.rank= -1, .disp = (MPI_Aint) MPI_BOTTOM}) {}

    template<typename T>
    T const& DistribNode<T>::getValue() const
    {
        return m_value;
    }

    template<typename T>
    void DistribNode<T>::setValue(const T& t_value)
    {
        m_value = t_value;
    }

    template<typename T>
    const DistribNodePtr &DistribNode<T>::getNext() const 
    {
        return m_next;
    }

    template<typename T>
    void DistribNode<T>::setNext(const DistribNodePtr &t_next) 
    {
        m_next = t_next;
    }
}
#endif //SOURCES_DISTRIBNODE_H
