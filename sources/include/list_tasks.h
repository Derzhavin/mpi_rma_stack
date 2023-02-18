//
// Created by denis on 12.02.23.
//

#ifndef SOURCES_LIST_TASKS_H
#define SOURCES_LIST_TASKS_H

template<typename ListImpl>
void runSimplePushPopTask(list_interface::IList<ListImpl>& list)
{
    int i = 3;
    list.pushBack(i);
    list.popBack();
}

#endif //SOURCES_LIST_TASKS_H
