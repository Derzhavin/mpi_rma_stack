//
// Created by denis on 02.02.23.
//

#include <iostream>
#include <type_traits>
#include <chrono>

#include "IStack.h"
#include "inner/InnerStack.h"
#include "logging.h"

using namespace std::literals::chrono_literals;

template<typename StackImpl>
using EnableIfValueTypeIsInt = std::enable_if_t<std::is_same_v<typename StackImpl::ValueType, int>>;

template<typename StackImpl,
        typename = EnableIfValueTypeIsInt<StackImpl>>
void runStackSimpleIntPushPopTask(stack_interface::IStack<StackImpl> &stack, MPI_Comm comm)
{
    SPDLOG_INFO("started 'runStackSimpleIntPushPopTask'");

    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    int size{0};
    MPI_Comm_size(comm, &size);

    const int elemsSize = 5;
    int elems[elemsSize]{0};

    for (int i = 0; i < elemsSize; ++i)
    {
        elems[i] = i * size + rank;
    }

    for (auto& elem: elems)
    {
        stack.push(elem);
        SPDLOG_DEBUG("received elem by 'push' {}", elem);
    }

    for (int i = 0; i < elemsSize; ++i)
    {
        int elem = stack.pop();
        SPDLOG_DEBUG("brought back elem by 'pop' {}", elem);
    }
}

void runInnerStackSimplePushPopTask(rma_stack::ref_counting::InnerStack &stack, int comm);

template<typename StackImpl,
        typename = EnableIfValueTypeIsInt<StackImpl>>
void runStackProducerConsumerBenchmarkTask(stack_interface::IStack<StackImpl> &stack, MPI_Comm comm,
                                           std::shared_ptr<spdlog::sinks::sink> loggerSink)
{
    SPDLOG_INFO("started 'runStackProducerConsumerBenchmarkTask'");

    auto pLogger = std::make_shared<spdlog::logger>(producerConsumerBenchmarkLoggerName.data(), loggerSink);
    pLogger->set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
    pLogger->flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

    const auto workload{1us};
    const auto totalOpsNum{15'000};

    auto procNum{0};
    MPI_Comm_size(comm, &procNum);
    const auto opsNum{totalOpsNum / procNum};

    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    const double tBeginSec = MPI_Wtime();
    if (rank % 2)
    {
        for (int i = 0; i < opsNum; ++i)
        {
            stack.push(i);
            std::this_thread::sleep_for(workload);
        }
    }
    else
    {
        for (int i = 0; i < opsNum; ++i)
        {
            stack.push(i);
            std::this_thread::sleep_for(workload);
        }
    }
    const double tEndSec = MPI_Wtime();

    const double workloadSec = std::chrono::duration_cast<std::chrono::microseconds>(workload).count() / 1'000'000.0f;
    const double tElapsedSec = tEndSec - tBeginSec - (opsNum * workloadSec);

    double tTotalElapsedSec{0};
    MPI_Allreduce(&tElapsedSec, &tTotalElapsedSec, 1, MPI_DOUBLE, MPI_MAX, comm);

    SPDLOG_LOGGER_INFO(pLogger, "procs {}, rank {}, elapsed (sec) {}, total (sec) {}", procNum, rank, tElapsedSec, tTotalElapsedSec);
}
