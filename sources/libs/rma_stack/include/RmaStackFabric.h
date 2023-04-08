//
// Created by denis on 04.04.23.
//

#ifndef SOURCES_RMASTACKFABRIC_H
#define SOURCES_RMASTACKFABRIC_H

#include "RmaTreiberCentralStack.h"

namespace rma_stack
{
    class RmaStackFabric
    {
    public:
        template<typename T>
        static RmaTreiberCentralStack<T> createRmaTreiberCentralStack(MPI_Comm comm, MPI_Datatype t_mpiDataType,
                                                                      const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                                      const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                                      int t_freeNodesLimit, MPI_Aint memoryPoolSize,
                                                                      std::shared_ptr<spdlog::sinks::sink> logger_sink);

    private:
        static MPI_Datatype registerCountedNodeMpiDatatype();
        static MPI_Datatype registerCentralNodeMpiDatatype();

    private:
        static bool m_sCountedNodeMpiDatatypeRegistered;
        static bool m_sCentralNodeMpiDatatypeRegistered;
    };

    template<typename T>
    RmaTreiberCentralStack<T> RmaStackFabric::createRmaTreiberCentralStack(MPI_Comm comm, MPI_Datatype t_mpiDataType,
                                                                           const std::chrono::nanoseconds &t_rBackoffMinDelay,
                                                                           const std::chrono::nanoseconds &t_rBackoffMaxDelay,
                                                                           int t_freeNodesLimit, MPI_Aint memoryPoolSize,
                                                                           std::shared_ptr<spdlog::sinks::sink> logger_sink) {
        auto pCentralizedMemoryPoolLogger = std::make_shared<spdlog::logger>("CentralizedMemoryPoolLogger", logger_sink);
        spdlog::register_logger(pCentralizedMemoryPoolLogger);
        pCentralizedMemoryPoolLogger->set_level(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));
        pCentralizedMemoryPoolLogger->flush_on(static_cast<spdlog::level::level_enum>(SPDLOG_ACTIVE_LEVEL));

        if (!m_sCountedNodeMpiDatatypeRegistered)
        {
            registerCountedNodeMpiDatatype();
        }
        if (!m_sCentralNodeMpiDatatypeRegistered)
        {
            registerCentralNodeMpiDatatype();
        }

        CentralizedMemoryPool<int> centralNodesMemoryPool(
                comm,
                t_mpiDataType,
                memoryPoolSize,
                std::move(pCentralizedMemoryPoolLogger)
        );

        CentralizedMemoryPool<int> countedNodesMemoryPool(
                comm,
                t_mpiDataType,
                memoryPoolSize,
                std::move(pCentralizedMemoryPoolLogger)
        );

        RmaTreiberCentralStack<T> stack(
                comm,
                t_mpiDataType,
                t_rBackoffMinDelay,
                t_rBackoffMaxDelay,
                t_freeNodesLimit,
                std::move(centralNodesMemoryPool)
        );

        return stack;
    }

} // rma_stack

#endif //SOURCES_RMASTACKFABRIC_H
