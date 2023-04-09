//
// Created by denis on 18.03.23.
//

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include <spdlog/sinks/dup_filter_sink.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <memory>

#include "include/logging.h"
#include "MpiException.h"
#include "include/centralized_memory_pool_tasks.h"

using namespace std::literals::string_literals;
using namespace std::chrono_literals;

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Info info = MPI_INFO_NULL;
    auto returnCode{EXIT_SUCCESS};

    MPI_Aint memoryPoolSize{100};
    MPI_Datatype datatype = MPI_INT;

    int rank{-1};
    MPI_Comm_rank(comm, &rank);

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

    auto pCentralizedMemoryPoolLogger = std::make_shared<spdlog::logger>("CentralizedMemoryPoolLogger", duplicatingFilterSink);
    spdlog::register_logger(pCentralizedMemoryPoolLogger);
    pCentralizedMemoryPoolLogger->set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
    pCentralizedMemoryPoolLogger->flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

    try
    {
        CentralizedMemoryPool memoryPool(comm, info, datatype, memoryPoolSize,
                                         std::move(pCentralizedMemoryPoolLogger));
        simpleMemoryAllocateAndDeallocateTask(memoryPool, comm, rank);
        SPDLOG_INFO("finished 'simpleMemoryAllocateAndDeallocateTask'");
    }
    catch (custom_mpi_extensions::MpiException& ex)
    {
        SPDLOG_ERROR("MPI exception - {}", ex.what());
        returnCode = EXIT_FAILURE;
    }
    catch (std::runtime_error& ex)
    {
        SPDLOG_ERROR("runtime exception - {}", ex.what());
        returnCode = EXIT_FAILURE;
    }
    catch (std::exception& ex)
    {
        SPDLOG_ERROR("unexpected exception - {}", ex.what());
        returnCode = EXIT_FAILURE;
    }
    MPI_Finalize();

    SPDLOG_INFO("shutdown");
    return returnCode;
}