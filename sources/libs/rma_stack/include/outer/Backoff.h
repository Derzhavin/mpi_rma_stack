//
// Created by denis on 19.02.23.
//

#ifndef SOURCES_BACKOFF_H
#define SOURCES_BACKOFF_H

#include <chrono>
#include <random>

namespace rma_stack
{
    class Backoff
    {
    public:
        Backoff(const std::chrono::nanoseconds &t_rMinDelayNs, const std::chrono::nanoseconds &t_rMaxDelayNs);

        void backoff();

    private:
        const std::chrono::nanoseconds m_maxDelayNs;
        int  m_limitDelayInt;

        std::mt19937 m_randomEngine;
    };

} // rma_stack

#endif //SOURCES_BACKOFF_H
