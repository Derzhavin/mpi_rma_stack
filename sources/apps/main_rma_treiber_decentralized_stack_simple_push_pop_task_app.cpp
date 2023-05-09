//
// Created by denis on 22.01.23.
//

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/dup_filter_sink.h>
#include <iostream>
#include <chrono>

#include "outer/RmaTreiberDecentralizedStack.h"
#include "include/stack_tasks.h"
#include "include/logging.h"

using namespace std::literals;

int main(int argc, char *argv[])
{
    auto returnCode{EXIT_SUCCESS};

    MPI_Init(&argc, &argv);
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Info info = MPI_INFO_NULL;

    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    const auto minBackoffDelay = 1ns;
    const auto maxBackoffDelay = 100ns;
    const auto elemsUpLimit{100};

    auto duplicatingFilterSink = std::make_shared<spdlog::sinks::dup_filter_sink_mt>(
            1s
    );

    auto loggingDefaultFilename = getLoggingFilename(rank, "default");
    auto fileDefaultSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
            loggingDefaultFilename.data()
    );
    duplicatingFilterSink->add_sink(fileDefaultSink);
    auto pDefaultLogger = std::make_shared<spdlog::logger>(defaultLoggerName.data(), duplicatingFilterSink);
    spdlog::set_default_logger(pDefaultLogger);
    spdlog::set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
    spdlog::flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

    try
    {
        auto rmaTreiberStack = rma_stack::RmaTreiberDecentralizedStack<int>::create(
                comm,
                info,
                minBackoffDelay,
                maxBackoffDelay,
                elemsUpLimit,
                duplicatingFilterSink
        );
        runStackSimpleIntPushPopTask(rmaTreiberStack, comm);
        rmaTreiberStack.release();
    }
    catch (custom_mpi_extensions::MpiException& ex)
    {
        SPDLOG_INFO("MPI exception"s + ex.what());
        returnCode = EXIT_FAILURE;
    }
    catch (std::exception& ex)
    {
        SPDLOG_INFO("Unexpected exception: "s + ex.what());
        returnCode = EXIT_FAILURE;
    }

    MPI_Finalize();
    return returnCode;
}