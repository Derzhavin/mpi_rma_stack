//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_ILIST_H
#define SOURCES_ILIST_H

namespace list_interface
{
    template<typename ListImpl>
    struct IList_traits;

    template<class ListImpl>
    class IList 
    {
        using ListTraitsImpl = IList_traits<ListImpl>;
        typedef typename ListTraitsImpl::ValueType ValueType;

    public:
        void pushBack(ValueType& value)
        {
            ListTraitsImpl::pushBackImpl(impl(), value);
        }
        void popBack()
        {
            ListTraitsImpl::popBackImpl(impl());
        }
        ValueType& tail()
        {
            return ListTraitsImpl::tailImpl(impl());
        }
        size_t getSize()
        {
            return ListTraitsImpl::getSize(impl());
        }
        bool isEmpty()
        {
            return ListTraitsImpl::isEmptyImpl(impl());
        }
    private:
        ListImpl& impl()
        {
            return static_cast<ListImpl&>(*this);
        }
    };

} // list_interface

#endif //SOURCES_ILIST_H
