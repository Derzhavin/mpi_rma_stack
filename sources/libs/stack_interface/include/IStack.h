//
// Created by denis on 22.01.23.
//

#ifndef SOURCES_ISTACK_H
#define SOURCES_ISTACK_H


namespace stack_interface
{
    template<typename StackImpl>
    struct IStack_traits;

    template<class StackImpl>
    class IStack
    {
        using StackTraitsImpl = IStack_traits<StackImpl>;
        typedef typename StackTraitsImpl::ValueType ValueType;

    public:
        void push(const ValueType &value)
        {
            StackTraitsImpl::pushImpl(impl(), value);
        }
        ValueType pop()
        {
            return StackTraitsImpl::popImpl(impl());
        }
        ValueType& top()
        {
            return StackTraitsImpl::topImpl(impl());
        }
        bool isEmpty()
        {
            return StackTraitsImpl::isEmptyImpl(impl());
        }
    private:
        StackImpl& impl()
        {
            return static_cast<StackImpl&>(*this);
        }
    };
} // stack_interface

#endif //SOURCES_ISTACK_H
