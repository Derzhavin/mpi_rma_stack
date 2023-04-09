//
// Created by denis on 22.01.23.
//

#include <iostream>
#include <chrono>
#include <spdlog/sinks/dup_filter_sink.h>
#include <spdlog/sinks/basic_file_sink.h>

#include "RmaCentralStackFabric.h"
#include "include/stack_tasks.h"
#include "include/logging.h"

using namespace std::literals;

int main(int argc, char *argv[])
{
    auto returnCode{EXIT_SUCCESS};

    MPI_Init(&argc, &argv);
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Info info = MPI_INFO_NULL;

    MPI_Comm_split_type(comm, MPI_COMM_TYPE_SHARED, 0 /* key */, MPI_INFO_NULL, &comm);

    int rank{-1};
    MPI_Comm_rank(comm, &rank);

    const auto minBackoffDelay = 1ns;
    const auto maxBackoffDelay = 100ns;
    const auto freeNodesLimit{10};
    const MPI_Aint memoryPoolSize{100};

    auto duplicatingFilterSink = std::make_shared<spdlog::sinks::dup_filter_sink_st>(
            1s
    );

    auto loggingFilename = getLoggingFilename(rank);
    auto fileSink = std::make_shared<spdlog::sinks::basic_file_sink_st>(
            loggingFilename.data()
    );
    duplicatingFilterSink->add_sink(fileSink);
    auto pDefaultLogger = std::make_shared<spdlog::logger>(defaultLoggerName.data(), duplicatingFilterSink);
    spdlog::set_default_logger(pDefaultLogger);
    spdlog::set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
    spdlog::flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

    try
    {
        auto rmaTreiberStack = rma_stack::RmaCentralStackFabric::getInstance().createRmaTreiberCentralStack<int>(
                comm,
                info,
                MPI_INT,
                minBackoffDelay,
                maxBackoffDelay,
                freeNodesLimit,
                memoryPoolSize,
                duplicatingFilterSink
        );
        runSimplePushPopTask(rmaTreiberStack);
    }
    catch (custom_mpi_extensions::MpiException& ex)
    {
        std::cerr << ex;
        returnCode = EXIT_FAILURE;
    }
    catch (std::exception& ex)
    {
        std::cerr << "Unexpected exception: " << ex.what();
        returnCode = EXIT_FAILURE;
    }

    MPI_Finalize();
    return returnCode;
}