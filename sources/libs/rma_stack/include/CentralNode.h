//
// Created by denis on 18.02.23.
//

#ifndef SOURCES_CENTRALNODE_H
#define SOURCES_CENTRALNODE_H

#include <mpi.h>

#include "CountedNode.h"

namespace rma_stack
{
    template<typename T>
    class CentralNode
    {
    public:
        explicit CentralNode(T t_value, CountedNode t_countedNode);
        explicit CentralNode(T t_value);

        T const& getValue() const;
        void setValue(const T& t_rValue);
        [[nodiscard]] CountedNode getCountedNode() const;
        void setCountedNode(CountedNode t_countedNode);

    private:
        CountedNode m_countedNode;
        T m_value{};
    };

    template<typename T>
    CentralNode<T>::CentralNode(T t_value, CountedNode t_countedNode)
    :
    m_value(std::move(t_value)),
    m_countedNode(t_countedNode)
    {}

    template<typename T>
    CentralNode<T>::CentralNode(T t_value)
    :
    CentralNode(std::move(t_value), CountedNode((MPI_Aint)MPI_BOTTOM))
    {}

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
    CountedNode CentralNode<T>::getCountedNode() const
    {
        return m_countedNode;
    }

    template<typename T>
    void CentralNode<T>::setCountedNode(CountedNode t_countedNode)
    {
        m_countedNode = t_countedNode;
    }
} // rma_stack

#endif //SOURCES_CENTRALNODE_H
