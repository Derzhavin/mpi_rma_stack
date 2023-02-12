//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_NODE_H
#define SOURCES_NODE_H

#include <mpi.h>

namespace rma_list
{
    struct NodePtr
    {
        int rank;
        MPI_Aint disp;

        void reset();
    };

    void NodePtr::reset()
    {
        disp = (MPI_Aint)MPI_BOTTOM;
        rank = -1;
    }

    template<typename T>
    class Node
    {
    public:
        explicit Node(T t_value, const NodePtr &t_next);
        explicit Node(T t_value);

        T const& getValue() const;
        void setValue(const T& t_value);
        [[nodiscard]] const NodePtr &getNext() const;

        void setNext(const NodePtr &t_next);

    private:
        T m_value{};
        NodePtr m_next{};
    };

    template<typename T>
    Node<T>::Node(T t_value, const NodePtr &t_next):m_value(t_value), m_next(t_next) {}

    template<typename T>
    Node<T>::Node(T t_value): Node(t_value, {.rank= -1, .disp = (MPI_Aint) MPI_BOTTOM}) {}

    template<typename T>
    T const& Node<T>::getValue() const
    {
        return m_value;
    }

    template<typename T>
    void Node<T>::setValue(const T& t_value)
    {
        m_value = t_value;
    }

    template<typename T>
    const NodePtr &Node<T>::getNext() const 
    {
        return m_next;
    }

    template<typename T>
    void Node<T>::setNext(const NodePtr &t_next) 
    {
        m_next = t_next;
    }
}
#endif //SOURCES_NODE_H
